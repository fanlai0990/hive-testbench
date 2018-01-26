# -*- coding: utf-8 -*-
from copy import deepcopy as deep_copy
from dag import DAG, DAGNode
from logical_plan import LogicalPlan, LogicalNode
from sql_operator import *


class NodeSet(object):
    """
    Helper class, stores the node in list, but provide the Set like api
    However, the time complexity are mostly O(N) instead of O(1)
    """
    def __init__(self, data_structure = None):
        if data_structure is None:
            self.__buffer = []
        else:
            self.__buffer = data_structure

    def add(self, node):
        if node not in self.__buffer:
            self.__buffer.append(node)

    def remove(self, node):
        self.__buffer.remove(node)

    def discard(self, node):
        for i, n in self.__buffer:
            if n is node:
                self.__buffer.pop(i)

    def __iter__(self):
        return iter(self.__buffer)


class PhysicalNode(DAGNode, DAG):
    VALID_TYPES ={MAP, REDUCE, COMBINE, SOURCE, SINK}.union(DAGNode.VALID_TYPES)

    def __init__(self, type, name, soft_factor=None, root=None,
                 net_node_name=None):
        assert root is not None
        if root.type is SOURCE:
            name = root.name
        super(self.__class__, self).__init__(type=type, name=name,
                                             soft_factor=soft_factor)
        self._root = root
        self._top_nodes = None
        self._leaves = None
        self._net_node_name = net_node_name

    def __str__(self):
        physical_str = super(self.__class__, self).__str__()
        net_str = "Corresponding NetNode: %s" %str(self._net_node_name)
        logical_str = super(self.__class__, self).str_helper()
        return '\n'.join((physical_str,net_str, 'Contains:', logical_str, ''))

    @property
    def net_node_name(self):
        assert self._net_node_name is not None
        return self._net_node_name

    @net_node_name.setter
    def net_node_name(self, val):
        self._net_node_name = val

    def is_soft_barrier(self):
        return self.type == COMBINE

    def is_hard_barrier(self):
        return self.type == REDUCE

    def update(self):
        # print "in phy node update"
        super(self.__class__, self).update()
        for node in self.top_nodes:
            if node.type is not SOURCE:
                node._in_stream = None
                # node._out_stream = None
            if node.type is COMBINE:
                node.fanout = len(node.sub_nodes)


    def is_barrier(self):
        return self.is_hard_barrier() or self.is_soft_barrier()

    def build(self):
        def remove_sub_node_diff_type(node, type = self.type):
            assert node.type is type, "%s(CheckType) != %s(NodeType)" %(
                node.type, type)
            sub_nodes = filter(lambda x: x.type is type, node.sub_nodes)
            # check correctness
            if len(sub_nodes) != len(node.sub_nodes):
                assert len(sub_nodes) == 0, ("len of sub_node after filter is "
                "not 0, but %d. Origin len of sub_node: %d") % (len(sub_nodes),
                                                            len(node.sub_nodes))
            node.sub_nodes = sub_nodes
        self.recursive_from_root(remove_sub_node_diff_type)
        self.flow_data()

    def reconnect_with_parent(self):
        assert len(self._root.prev_nodes) != 0, "try to merge node without parents"
        for node in self._root.prev_nodes:
            node.insert_sub_node(self._root)

    def merge_sub_node(self, sub_node, type=None):
        if type is not None:
            self.type = type
        sub_node.reconnect_with_parent()
        self.unlink_node(parent=self, child=sub_node)

    def merge_prev_node(self, prev_node=None):
        if prev_node == None:
            assert len(self.prev_nodes) == 1
            prev_node = self.prev_nodes[0]
        self.reconnect_with_parent()
        self.unlink_node(prev_node, self)
        for node in self.sub_nodes[:]:
            self.partial_unlink_node(parent=self, child=node)
            self.link_node(prev_node, node)

    def unlink_to_parent(self, total_unlink=False):
        for prev_node in self.prev_nodes:
            self.partial_unlink_node(prev_node, self, double_link=total_unlink)

    def partial_link_node(self, parent, child, parent_lnode=None, double_link=False):
        '''
        Partially link the parent node with the child node by default. Previous
        link's sub node has an edge connect to the prev link, not vise versa.
        '''
        self.link_node(parent, child)
        child_lnode = child._root
        leaves = parent.leaves
        if parent_lnode is not None:
            leaves = [parent_lnode]
        for node in leaves:
            if node not in child_lnode.prev_nodes:
                child_lnode.insert_prev_node(node)
            if double_link and child_lnode not in node.sub_nodes:
                node.insert_sub_node(child_lnode)

    def partial_unlink_node(self, parent, child, double_link=False):
        '''
        This is a partial unlink for the sub nodes, since we need to maintain
        the sub to prev logical node for future usage, if want a complete unlink
        , call the self.unlink
        '''
        self.unlink_node(parent, child)
        child_lnode = child._root
        for node in parent.leaves:
            child_lnode.remove_prev_node(node)
            if double_link and child_lnode in node.sub_nodes:
                node.remove_sub_node(child_lnode)

    def __assert_copy_support(self):
        '''
        It only make sense to make a copy of the physical node when it only
        has one single leaf. In fact, right now we only supports for node
        copy of all COMBINEs, all MAP, several COMBINEs+several MAPs constructions.
        '''
        supporting_types = {COMBINE, MAP}
        assert self.type in supporting_types, "Not a supportive type for ," \
        "copying. Supporting: %s, this type: %s" %(",".join(supporting_types),
                                                   self.type)
        if len(self) > 1:
            curr_type = self._root.type
            for node in self.top_nodes:
                assert len(node.sub_nodes) <= 1, "Every lnode in the pnode" \
                            "should only contain 1 child, however, this lnode:"\
                            "%s has length: %d" % (str(node), len(self))
                if node.type != curr_type:
                    assert node.type is COMBINE
                    curr_type = COMBINE

    def __make_copy(self, count, type=None):
        '''Helper function to make the copy of self.
        All edges are removed for simplicity, and can setup back by others
        '''
        def copy_and_unlink_subs(node, sub_nodes):
            sub_copy = sub_nodes[:]
            for sub_node in sub_copy:
                # partial unlink is enough, since it not affects the copy
                self.unlink_node(node, sub_node)
            assert len(sub_nodes) == 0
            return sub_copy

        def copy_and_unlink_prevs(node, prev_nodes):
            prev_copy = prev_nodes[:]
            for prev_node in prev_copy:
                self.unlink_node(prev_node, node)
            assert len(prev_nodes) == 0
            return prev_copy

        self.__assert_copy_support()
        # store the sub & prev physical nodes and logical node for future reconnection
        prev_nodes = copy_and_unlink_prevs(self, self._prev_nodes)
        sub_nodes = copy_and_unlink_subs(self, self._sub_nodes)
        # store the prev logical node for future reconnection
        assert len(self._root.prev_nodes) > 0
        prev_lnodes = self._root.prev_nodes[:]
        self._root._prev_nodes = []
        # make copy
        copy_node = deep_copy(self)
        if type is None:
            type = self.type
        copy_node._name = "_".join([type, str(count)])
        copy_node.type = type
        return copy_node, prev_lnodes, sub_nodes, prev_nodes

    def vertical_prev_copy(self, count, type=None):
        """return a copy of the current node, make it the only prev node of the
        current node. We cannot make the sub_copy since the sub nodes' root is
        connected to the self.leaves
        """
        copy_node, prev_lnodes, sub_nodes, prev_nodes = self.__make_copy(count,
                                                                         type)
        # reset edges
        for sub_node in sub_nodes:
            self.link_node(self, sub_node)
        for prev_node in prev_nodes:
            self.link_node(prev_node, copy_node)
        copy_node._root._prev_nodes = prev_lnodes
        self.link_node(parent=copy_node, child=self)
        self.update()
        return copy_node

    def map_horizontal_copy(self, count, type=None):
        '''
        Make copy for map nodes.
        After make the copy: self node is connected to the prev node, and
        connect to the sub nodes. The copy node only connect to the prev node
        not the sub node, for simplicity.
        '''
        copy_node, prev_lnodes, sub_nodes, prev_nodes = self.__make_copy(count,
                                                                         type)
        # link back the prev nodes for both self and parent
        for prev_node in prev_nodes:
            self.partial_link_node(prev_node, self)
            self.partial_link_node(prev_node, copy_node)
        # sanity assertion, not guarantee correctness
        for lnode in prev_lnodes:
            assert lnode in self._root.prev_nodes
            assert lnode in copy_node._root._prev_nodes
        # reconnect the sub nodes
        for sub_node in sub_nodes:
            self.link_node(self, sub_node)
        self.update()
        return copy_node

    def combine_copy(self, node_list, count):
        assert len(self.top_nodes) == 1, "this pnode: %s" %str(self)
        copy_node, prev_lnodes, sub_nodes, prev_nodes = self.__make_copy(count)
        # remove all sub_node root's prev links
        map(lambda x: self.link_node(self, x), sub_nodes)
        map(lambda x: self.partial_unlink_node(self, x), sub_nodes)
        # link the nodes in nodelist to the copy node
        for pnode in node_list:
            self.partial_link_node(copy_node, pnode)
            sub_nodes.remove(pnode)
        # link the rest of sub_nodes to original node
        map(lambda x: self.partial_link_node(self, x), sub_nodes)
        # link the copy node to aggregate node
        self.partial_link_node(self, copy_node)
        #
        map(lambda x: self.partial_link_node(x, self), prev_nodes)
        # sanity check
        for lnode in prev_lnodes:
            assert lnode in self._root.prev_nodes
        return copy_node

    def __cal_out_stream(self):
        if self.type is COMBINE:
            self.fanout = 1 / float(len(self.sub_nodes))
            self._out_stream = self.fanout * self._in_stream
            self.root._out_stream = self._out_stream
        else:
            self._out_stream = self.root.out_stream

    def re_flow_data(self):
        def flow_data(dag_node):
            # a wrapper for data flow in nodes. This wrapper is a walk around to make COMBINE node re-construct itself
            # its fan_out
            if dag_node.type is COMBINE:
                # assert dag_node.fanout >1
                if dag_node.fanout > 1 :
                    dag_node.fanout = 1 / float(dag_node.fanout)
                input_stream = reduce(lambda r, x: r + x.out_stream,
                                      dag_node.sub_nodes, 0)
                dag_node._in_stream = input_stream if dag_node._in_stream is None else dag_node._in_stream + input_stream
            dag_node.flow_data()

        assert self.root is not None, "physical node root is None"
        self._in_stream = super(self.__class__, self)._cal_in_stream()
        map(lambda x: flow_data(x), reversed(self.top_nodes))
        self._out_stream = self.root.out_stream
        # TODO: resolve the physical node fanout updates
        # self.fanout = float(self.out_stream) / float(self.in_stream)
        for prev_lnode in self.root.prev_nodes:
            if prev_lnode._in_stream is None:
                prev_lnode._in_stream = self.root.out_stream
            else:
                prev_lnode._in_stream += self.root.out_stream
            if prev_lnode.type is COMBINE:
                prev_lnode.fanout += 1
            # debug
            # print "prev lnode"
            # print str(prev_lnode)
            # print "prev node"
            # assert len(self.prev_nodes) == 1
            # print str(self.prev_nodes[0])
            # print "self"
            # print str(self)


    def flow_data(self):
        self._in_stream = super(self.__class__, self)._cal_in_stream()
        self.__cal_out_stream()


class PhysicalPlan(DAG):
    def __init__(self, logical_plan=None, ref_node_map=None, source_map=None,
                 source2net_map=None, destination_node=None):
        super(self.__class__, self).__init__(self)
        self._logical_plan = deep_copy(logical_plan)
        if ref_node_map is not None and source_map is not None:
            self._logical_plan = LogicalPlan(ref_node_map=ref_node_map,
                                             source_map=source_map)
            self._logical_plan.build()
        assert self._logical_plan is not None, \
            "logical Plan is None after init, logical_plan: %s, " \
            "ref_node_map:%s" "source_map: %s" % (str(logical_plan),
                                    str(ref_node_map), str(source_map))
        self._logical_root = self._logical_plan.root
        self._logical_top_nodes = self._logical_plan.top_nodes[:]
        self._node_counter = 0
        self._root = PhysicalNode(type=ROOT,name=ROOT,root=self._logical_root,
                                  net_node_name=destination_node)
        self._source2net_map = source2net_map
        self._source_nodes = None
        self._source_node_map = None
        self._is_built = False
        self._is_geo_built = False
        self._nop_root_processed = False
        self._nop_root = None

    @property
    def source2net_map(self):
        return self._source2net_map

    @source2net_map.setter
    def source2net_map(self, val):
        self._source2net_map = val

    @property
    def source_nodes(self):
        super(self.__class__, self).update()
        if self._source_nodes is None:
            self._source_nodes = filter(lambda x: x.type == SOURCE,
                                        self.top_nodes)
        return self._source_nodes

    @property
    def source_node_map(self):
        self.update()
        if self._source_node_map is None:
            self._source_node_map = {}
            for source_node in self.source_nodes:
                self._source_node_map[source_node.name] = source_node
        return self._source_node_map

    @property
    def node_counter(self):
        self._node_counter += 1
        return self._node_counter

    @property
    def is_geo_built(self):
        return self._is_geo_built

    @property
    def is_built(self):
        return self._is_built

    def get_partitions(self):
        return map(lambda x: x.name, self.source_nodes)

    def get_geo_plan(self):
        geo_plan = {}
        self.update()
        for pnode in self.top_nodes:
            if pnode.net_node_name not in geo_plan:
                geo_plan[pnode.net_node_name] = [pnode]
            else:
                geo_plan[pnode.net_node_name].append(pnode)
        return geo_plan

    def update(self):
        # print "update in phy plan"
        super(self.__class__, self).update()
        self._source_nodes = None
        a = self.top_nodes
        # print str(self)
        map(lambda x: x.update(), self.top_nodes)
        map(lambda x: x.re_flow_data(), reversed(self.top_nodes))

    def build(self):
        assert self._is_built is False, "Build after being built"
        # transform the logical node to physical types
        map(lambda x: self.logical2physical_transform(x), self._logical_top_nodes)
        # recursively create the physical node and assign the logical node root,
        # merge the same logical type nodes
        self.merge_nodes()
        # build the node from root to leaves
        map(lambda x: x.build(), self.top_nodes[::-1])
        self._is_built = True

    def geo_build(self, source2net_map=None):
        assert self._is_geo_built is False, "Re-Geo build after built"
        if source2net_map is not None:
            assert self.source2net_map is None
            self.source2net_map = source2net_map
        self.merge_source_nodes()
        self._nop_root = deep_copy(self.root)
        self.sink_merge_reduce()
        # print "after sink_merge_reduce"
        self.update()
        self._is_geo_built = True

    def group_source_by_aggregate(self, source_node, parent_child_kv_list):
        found_flag = False
        assert len(source_node.prev_nodes) == 1, str(source_node)
        cur_node = source_node.prev_nodes[0]
        while (cur_node.type is not COMBINE):
            if len(cur_node.prev_nodes) != 1:
                parent_child_kv_list.append((None, [source_node]))
                return parent_child_kv_list
            cur_node = cur_node.prev_nodes[0]
        parent_node = cur_node
        for parent, child_list in parent_child_kv_list:
            if parent_node is parent:
                child_list.append(source_node)
                found_flag = True
        if not found_flag:
            parent_child_kv_list.append((parent_node, [source_node]))
        return parent_child_kv_list

    def source_group_nodes(self):
        '''
        Group the source nodes by the parent aggregate nodes
        '''
        # procedure functions
        def get_names(source_nodes):
            return map(lambda x: x.name, source_nodes)
        # logic procedures
        source_nodes = self.leaves[:]
        parent_child_kv_list = reduce(lambda kv_list, source_node:
                                          self.group_source_by_aggregate(source_node, kv_list),
                                          source_nodes,
                                          [])
        return map(lambda x: get_names(x[1]), parent_child_kv_list)

    def get_parent(self, pnodes, net_name):
        print "@get_parent, net_name: %s, pnodes: %s\n ---------END---------" %(net_name, "--------------".join(map(lambda x: str(x), pnodes)))
        try:
            print "Parent: %s" % str(pnodes[0].prev_nodes[0])
        except Exception as e:
            pass
        parent_child_kv_list = reduce(lambda kv_list, source_node:
                                      self.group_source_by_aggregate(source_node, kv_list),
                                      pnodes,
                                      [])
        def check_lineage(pnode1, pnode2):
            # TODO: Given two physical node, check their lineages
            return False

        def group_pnode_by_net_name(pnodes):
            net2pnode_dict = {}
            for pnode in pnodes:
                if pnode.net_node_name not in net2pnode_dict:
                    net2pnode_dict[pnode.net_node_name] = [pnode]
                else:
                    net2pnode_dict[pnode.net_node_name].append(pnode)
            return net2pnode_dict

        def process_one_parent_child_kv(parent_child_kv_item):
            def get_parent_node_made_simple(parent_node, pnodes, merge_parent_pnodes=False):
                print "@get_parent_node_made_simple, merge_parent_pnodes: %s" %str(merge_parent_pnodes)
                ret_node = parent_node
                if len(parent_node.sub_nodes) != len(pnodes) and len(pnodes) > 1:
                    aggregate_copy = aggregate_node.combine_copy(node_list=pnodes,
                                                                 count=self.node_counter)
                    ret_node = aggregate_copy
                if merge_parent_pnodes:  # merge with the parent node if all map to the same net name
                    assert len(pnodes) > 0, "len of pnodes: %d" %len(pnodes)
                    for pnode in pnodes:
                        # TODO: Fix bug
                        # assert parent_node is pnode.prev_nodes[0], "parent: %s\n first pnode parent: %s\n pnode: %s" %(str(parent_node), str(pnode.prev_nodes[0]), str(pnode))
                        pnode.merge_prev_node()
                    ret_node.net_node_name = pnodes[0].net_node_name
                return ret_node


            aggregate_node = parent_child_kv_item[0] # by setting, index = 0 is a single aggregate node
            aggregate_sub_nodes = parent_child_kv_item[1] # by setting, index = 1 is a list of sub nodes
            if aggregate_node is None or aggregate_node.type in {ROOT, SINK}:
                return None
            else:
                assert aggregate_node.type is COMBINE
                # take into consideration that the given pnodes may not be the all sub_nodes the aggregate_node has
                aggregate_node = get_parent_node_made_simple(aggregate_node, aggregate_sub_nodes, False)
                # group the aggregate_sub_nodes by the net_names
                net_name2pnodes_dict = group_pnode_by_net_name(aggregate_sub_nodes)
                # returned node, the parent
                ret_node = aggregate_node
                if net_name in net_name2pnodes_dict:
                    ret_node = get_parent_node_made_simple(aggregate_node, net_name2pnodes_dict[net_name], True)
                    if len(net_name2pnodes_dict[net_name]) == len(aggregate_sub_nodes):
                        # case 1, where pnodes and aggregate node are merge to the same node
                        print "case 1"
                        ret_node.type = SOURCE
                        print str(ret_node)
                    else:
                        print "case 2"
                        # case 2, where part of the sub nodes and the parent node are within the same node, whereas
                        # the other parts are not
                        ret_node.type = COMBINE
                else:
                    # cover case 3, 4
                    print "case 3 or 4"
                    for this_net_name, this_pnodes in net_name2pnodes_dict.iteritems():
                        print "this_net_name: %s, this pnodes: %s" %(this_net_name, ",".join(map(lambda x: str(x), this_pnodes)))
                        get_parent_node_made_simple(aggregate_node, this_pnodes, len(this_pnodes) > 1)
                ret_node.net_node_name = net_name
                self.update()
                if len(ret_node.sub_nodes) == 0:
                    ret_node.type = SOURCE
                print "ret_node: %s" %str(ret_node)
                print "End @get_parent"
                return ret_node

        if len(parent_child_kv_list) == 1:
            return process_one_parent_child_kv(parent_child_kv_list[0])
        elif len(parent_child_kv_list) < 1:
            return None
        else:
            parent_node_none_decider = reduce(lambda r, x:
                                              x[0] if x[0] is not None else r,
                                              parent_child_kv_list, None)
            if parent_node_none_decider is None:
                return None
            else:
                # TODO: if multiple aggregate node converge to one single merge nodes
                # self.update()
                print "==============================================="
                for k, v in parent_child_kv_list:
                    print "--------------kv-list-------------------\n"
                    print str(k), "\n".join(map(lambda x: str(x), v))
                raise Exception("not suppotted multiple group node converge")

    def logical2physical_transform(self, node):
        if node.type == ROOT or node.type == SOURCE:
            return
        if node.is_barrier():
            if node.is_soft_barrier():
                node.type = COMBINE
            else:
                node.type = REDUCE
        else:
            node.type = MAP


    def merge_nodes(self):
        '''Merge multiple level node into one physical node(physical stage)
        This is a initial merge, where the merges only happen in between levels.
        Partial merges is not implemented in this setup. Moreover, multi-level
        reduce is only merged if the number of nodes is one of both of the level
        which exclude the bushy-join possible.
        This merge follows the following rules:
            pass
        '''
        for lnode in self._logical_root.sub_nodes:
            self.__recursive_merge(self._root, lnode, True)


    def __recursive_merge(self, pnode, lnode, is_new_pnode):
        this_pnode = pnode
        if is_new_pnode:
            # create current physical node, connect it into the graph
            this_pnode = PhysicalNode(type=lnode.type,
                        name="_".join((lnode.type, str(self.node_counter))),
                        root=lnode)
            self.link_node(pnode, this_pnode)
        # Create new physical nodes for next level if any logical node of next
        # level is not the same type of the current logical node
        same_nodes = filter(lambda x: x.type == lnode.type, lnode.sub_nodes)
        next_is_new_pnode = len(same_nodes) != len(lnode.sub_nodes) or lnode.is_barrier()
        map(lambda x: self.__recursive_merge(this_pnode, x, next_is_new_pnode), lnode.sub_nodes)


    def group_by_parent(self, source_node, parent_child_kv_list):
        found_flag = False
        assert len(source_node.prev_nodes) == 1
        parent_node = source_node.prev_nodes[0]
        for parent, child_list in parent_child_kv_list:
            if parent_node is parent:
                child_list.append(source_node)
                found_flag = True
        if not found_flag:
            parent_child_kv_list.append((parent_node, [source_node]))
        return parent_child_kv_list


    def merge_source_nodes(self):
        def stage_source_nodes_with_map(net_name, source_nodes, map_node):
            for source_node in source_nodes:
                # source_node.unlink_to_parent()
                source_lnode_parent = map_node.root
                while (len(source_lnode_parent.sub_nodes) == 1
                       and source_lnode_parent.sub_nodes[0].type is MAP):
                    source_lnode_parent = source_lnode_parent.sub_nodes[0]
                for node in source_node.prev_nodes:
                    if node is not map_node:
                        map_node.partial_unlink_node(node, source_node, double_link=True)
                map_node.partial_link_node(parent=map_node, child=source_node,
                                           parent_lnode=source_lnode_parent, double_link=True)
                map_node.unlink_node(map_node, source_node)
            map_node.net_node_name = net_name
            map_node.flow_data()

        # assertion checks
        partition_count = len(self._logical_plan.source_map.keys())
        assert partition_count == len(self.leaves), "num partitions: %d, " \
            "num leaves:%d" %(partition_count, len(self.leaves))
        assert self.source2net_map is not None, "@merge_source_nodes"
        # group and created the geo source nodes
        original_map_nodes = NodeSet([])
        for net_name in self.source2net_map:
            source_nodes = map(lambda x: self.source_node_map[x],
                               self.source2net_map[net_name])
            # Gather the source nodes in one net_node into groups by their parent
            parent_child_kv_list = reduce(lambda kv_list, source_node:
                                          self.group_by_parent(source_node, kv_list),
                                          source_nodes,
                                          [])

            for map_node, source_nodes in parent_child_kv_list:
                original_map_nodes.add(map_node)
                map_copy = map_node.map_horizontal_copy(count=self.node_counter,
                                                        type=SOURCE)
                stage_source_nodes_with_map(net_name, source_nodes, map_copy)
        # remove the link of the original map nodes
        for map_node in original_map_nodes:
            map_node.unlink_to_parent(total_unlink=True)
            for sub_node in map_node.sub_nodes:
                map_node.partial_unlink_node(map_node, sub_node)


    def sink_merge_reduce(self):
        #Merge the reduce nodes that are solely the only node under the sink node
        while (len(self._root.sub_nodes)==1):
            if self.merge_first_node_under_root(REDUCE) is False:
                return


    def merge_first_node_under_root(self, check_type):
        assert len(self.root.sub_nodes) > 0
        sub_node = self.root.sub_nodes[0]
        if sub_node.type != check_type:
            return False
        else:
            sub_node.merge_prev_node()
            return True

    def sink_merge_all_non_reduce(self, root):
        def merge_non_source(node):
            node.merge_prev_node()

        self.post_order_recursive_dispatcher(self._nop_root, merge_non_source)

    def aggregate_push_down(self, pnode):
        pass
        # TODO: This work is been done in get_parent and map operator to source
        # if is soft barrier, push upper_node to prev reduce, push lower_node to
        # sub maps
