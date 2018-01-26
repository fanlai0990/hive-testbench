# -*- coding: utf-8 -*-
from . import DAG, DAGNode
from sql_operator import *
class LogicalNode(DAGNode):
    """Single node of query the query DAG"""
    VALID_TYPES ={JOIN, UNION, SINK, UNIQUE, AGGREGATE, FILTER, SOURCE,
                    LIMIT}.union(DAGNode.VALID_TYPES)

    def __init__(self, type, name, fanout = 1, in_stream = None,
                 out_stream = None, soft_factor = None, prev_node=None):
        super(self.__class__, self).__init__(type=type, name=name, fanout=fanout,
            in_stream=in_stream,out_stream=out_stream, soft_factor=soft_factor)
        if prev_node is not None:
            self.insert_prev_node(prev_node)

    def is_barrier(self):
        return self.is_hard_barrier() or self.is_soft_barrier()

    def is_hard_barrier(self):
        """Returns true if this node is a hard barrier that require a
        global shuffle immediately before processing this query node.
        All sub node result must fetched to a single NetNode before
        processing this query node
        """
        return self.type in {JOIN, UNION, SINK}

    def is_soft_barrier(self):
        """Returns true if this node is a soft barrier that requires
        a global shuffle but can be split into multiple shuffle-reduce
        stages which can minimize the out_stream from a NetNode.
        But still need to fetch all the result to a single NetNode before
        processing this query node.
        """
        return self.type in {UNIQUE, AGGREGATE}


class LogicalPlan(DAG):
    def __init__(self, ref_node_map, source_map, root=None):
        super(self.__class__, self).__init__(root=root)
        self.ref_node_map = ref_node_map
        self.source_map = source_map

    def build(self):
        # construct the original graph
        self._root = self.__top_down_build()
        super(self.__class__, self).build()

    def update(self):
        super(self.__class__, self).update()
        for node in self.top_nodes:
            if node.type is not SOURCE:
                node._in_stream = None

    def optimize(self, rules = None):
        """May apply rules for optimization"""
        pass
        assert self._root is not None, "original_root is None. Probably" \
                                       " optimized before build"
        for rule in rules:
            # apply rules on logical plans
            map(lambda x: rule(x), self.top_nodes)

    def __top_down_build(self):
        root = LogicalNode(type=ROOT, name=ROOT)
        for node_name in self.ref_node_map[ROOT]["child"]:
            root.insert_sub_node(self.__recursive_build(root, node_name))
        return root


    def __recursive_build(self, parent, node_name):
        # if is not source node
        cur_node = None
        if self.ref_node_map.has_key(node_name):
            cur_ref_node = self.ref_node_map[node_name]
            cur_node = LogicalNode(type=cur_ref_node["type"], name=node_name,
                            fanout=cur_ref_node["fanout"], prev_node=parent)
            for node_name in cur_ref_node["child"]:
                cur_node.insert_sub_node(self.__recursive_build(cur_node, node_name))
        else:
            cur_node = LogicalNode(type=SOURCE, name=node_name,
                        prev_node=parent, in_stream=self.source_map[node_name])
        assert cur_node is not None, "node is none in recursive building"
        return cur_node