# -*- coding: utf-8 -*-
from logical_plan import LogicalPlan
from physical_plan import PhysicalPlan
from copy import deepcopy as deep_copy
from sql_operator import *

import sys

sys.path.append("..")
from utils import *

import heapq

class _entry(object):
    def __init__(self, pns, parent, children, dist):
        self.pns = pns
        self.parent = parent
        self.children = children
        self.dist = dist

class QueryPlanner(object):
    """Query Planner will change the """
    def __init__(self, QueryPlanJson=None, DataSourceJson=None, soft_factor=None):
        self.cid_to_pnode = {}
        self.path = {}

    def generate_plan(self, list_of_pps, net_graph):
        assert len(list_of_pps) > 0
        root = list_of_pps[0]._root.net_node_name
        leaves = []
        self.get_leaves(list_of_pps[0]._root, leaves)
        plan = self.shortest_path_to_all_nodes(root, net_graph)

        res_plan = deep_copy(plan)
        count = 0
        flag = False
        for pp in list_of_pps:
            if self.evaluate_pp(pp._root, plan, root):
                if len(pp._top_nodes) > count:
                    count = len(pp._top_nodes)
                    res_plan = deep_copy(plan)
                flag = True

        if not flag:
            return None
        # print "@@@res_plan"
        # for (key, value) in res_plan.items():
        #     print key, value.pns
        return res_plan

    def get_leaves(self, pp_node, leaves):
        if pp_node._type == SOURCE:
            leaves.append(pp_node.net_node_name)
        else:
            for node in pp_node._sub_nodes:
                self.get_leaves(node, leaves)

    def evaluate_pp(self, pp, plan, root):
        # TODO: call evaluate_node with the root of the physical plan
        if self.map_node(pp, plan, root) is None:
            return False
        else:
            return True

    def map_node(self, node, plan, root):
        if node._type == SOURCE:
            if plan[node.net_node_name].pns:
                return None
            plan[node.net_node_name].pns = node
            return node.net_node_name

        cid_of_children = []
        for child in node._sub_nodes:
            cid = self.map_node(child, plan, root)
            if cid is None:
                return None
            cid_of_children.append(cid)

        cid = self.lowest_common_ancestor(cid_of_children, plan, root)
        if cid == root and node._type != ROOT:
            return None
        node.net_node_name = cid
        if plan[cid].pns:
            return None
        plan[cid].pns = node
        return cid

    def lowest_common_ancestor(self, list_of_cid, plan, root):
        common_path = self.get_path(plan, root, list_of_cid[0])
        for i in range(1, len(list_of_cid)):
            path = self.get_path(plan, root, list_of_cid[i])
            tmp = []
            for i in range(len(common_path)):
                if i >= len(path):
                    break
                else:
                    if common_path[i] == path[i]:
                        tmp.append(common_path[i])
                    else:
                        break
            common_path = tmp

            if len(common_path) == 1:
                break

        return common_path[-1]

    def get_path(self, plan, root, cid):
        path = [cid]
        while plan[cid].parent is not None:
            cid = plan[cid].parent
            path.insert(0, cid)

        return path

    # TODO: recursively generate the cluster id for a node
    def evaluate_node(self, node, netGraph):
        if node.type is SOURCE:
            assert node.net_node_name is not None
            return node.net_node_name

        cid_of_children = []
        for child in node.sub_nodes:
            cid, _= self.evaluate_node(child, netGraph)
            cid_of_children.append(cid)

        # generate cid of current node
        result = []
        for cid in netGraph.nodes.keys():
            # find best solution by calling SteinerTree()
            weight, tree = self.SteinerTree(cid_of_children+[cid], netGraph)
            result.append((cid, weight, tree))

        index = None
        best = float("inf")
        for i in range(len(result)):
            if result[i][1] < best:
                best = result[i][1]
                index = i

        self.cid_to_pnode[result[index][0]] = node

        return (result[index][0], result[index][2])

    def shortest_path_to_all_nodes(self, source, net_graph):
        dist = {}
        dist[source] = [0.0, None]
        unvisited = [source]

        for node in net_graph.nodes:
            if node != source:
                dist[node] = [float("inf"), ""]
                unvisited.append(node)

        while unvisited:
            # find the node with smallest dist
            node = reduce(lambda x, y : y if dist[x][0] > dist[y][0] else x, unvisited)
            unvisited.remove(node)
            # relax all neighbor nodes
            for neighbor, nInfo in net_graph.links[node].items():
                if neighbor in unvisited and dist[node][0] + 1/nInfo["bandwidth"] < dist[neighbor][0]:
                    dist[neighbor][0] = dist[node][0] + 1/nInfo["bandwidth"]
                    dist[neighbor][1] = node

        # forming the result dict
        res = dict()
        for (node, pair) in dist.items():
            if node in res:
                assert res[node].parent == None
                res[node].parent = pair[1]
                res[node].dist = pair[0]
            else:
                res[node] = _entry(None, pair[1], [], pair[0])

            par = pair[1]
            if par in res:
                assert node not in res[par].children
                res[par].children.append(node)
            else:
                res[par] = _entry(None, None, [node], None)

        return res

    def SteinerTree(self, list_of_cluster_id, root, net_graph):
        # TODO: return a dict
        # key is the added cluster
        # value is a list of clusters connected to
        # the added cluster
        weight = 0.0
        stree = dict()

        target_set = [root]
        stree[root] = []
        print "stiner tree debug: ", str(list_of_cluster_id)
        for i in reversed(range(0, len(list_of_cluster_id))):
            weight += self.shortest_path_to_a_set_of_nodes(list_of_cluster_id[i], target_set, net_graph, stree)
            print "stree[%d]: %s" %(i, str( stree))
            print "target_set", target_set

        print "stree"
        print stree
        geo_plan = dict()
        self.build_tree(net_graph, stree, root, None, [root], geo_plan, 0)
        return geo_plan

    def build_tree(self, net_graph, stree, root, prev, visited, geo_plan, weight):
        geo_plan[root] = _entry([], prev, [], weight) # pns, parent, children, dist
        for child in stree[root]:
            if child not in visited:
                visited.append(child)
                geo_plan[root].children.append(child)
                print root, child
                bw = net_graph.links[root][child]["bandwidth"]
                self.build_tree(net_graph, stree, child, root, visited, geo_plan, weight + 1/bw)


    def shortest_path_to_a_set_of_nodes(self, source, target_set, net_graph, stree):
        if source in stree:
            return 0.0
        dist = {}
        dist[source] = [0.0, None]
        unvisited = [source]

        for node in net_graph.nodes:
            if node != source:
                dist[node] = [float("inf"), ""]
                unvisited.append(node)
        target_node = None
        while unvisited:
            node = reduce(lambda x, y : y if dist[x][0] > dist[y][0] else x, unvisited)
            unvisited.remove(node)
            if node in target_set:
                target_node = node
                break
            # relax all neighbor nodes
            for neighbor, nInfo in net_graph.links[node].items():
                if neighbor in unvisited and dist[node][0] + 1/nInfo["bandwidth"] < dist[neighbor][0]:
                    dist[neighbor][0] = dist[node][0] + 1/nInfo["bandwidth"]
                    dist[neighbor][1] = node

        print "target_node:", target_node
        assert target_node != None

        weight = 0
        tn = target_node
        while tn != None:
            if tn not in target_set:
                target_set.append(tn)
            if dist[tn][1] is not None:
                if tn in stree:
                    if dist[tn][1] not in stree[tn]:
                        stree[tn].append(dist[tn][1])
                else:
                    stree[tn] = [dist[tn][1]]
                if dist[tn][1] in stree:
                    if tn not in stree[dist[tn][1]]:
                        stree[dist[tn][1]].append(tn)
                else:
                    stree[dist[tn][1]] = [tn]
            weight += dist[tn][0]
            tn = dist[tn][1]

        print "source", source
        print "stree", stree
        assert source in stree

        return weight

    def place_partitions(self, network, ls_of_ls_pt, plan, alg = None):
        nodes = network.nodes
        cluster_to_partitions = dict()

        alg.initialize(plan)
        # cluster_to_pass = dict()
        # for (c, value) in plan.items():
        #     cluster_to_pass[c] = value.dist

        for ls_pt in ls_of_ls_pt:
            while (len(ls_pt) > 0):
                max_intersect = set([])
                partial_res = []
                for (key, value) in nodes.items():
                    t = set(value.partition).intersection(ls_pt)
                    if len(t) > len(max_intersect):
                        max_intersect = t.copy()
                assert max_intersect is not None

                for (key, value) in nodes.items():
                    t = set(value.partition).intersection(ls_pt)
                    if t == max_intersect:
                        partial_res.append(key)

                res = alg.find(partial_res)
                # res = None
                # if len(partial_res) == 1:
                #     res = partial_res[0]
                # elif len(partial_res) > 1:
                #     res = reduce(lambda x, y: y if cluster_to_pass[x] > cluster_to_pass[y] else x, partial_res)
                # else:
                #     raise Exception("Does not found partitions in DCs")

                if res in cluster_to_partitions:
                    cluster_to_partitions[res].extend(list(max_intersect))
                else:
                    cluster_to_partitions[res] = list(max_intersect)

                alg.update(res, plan, max_intersect)
                # # update stride pass
                # cluster_to_pass[res] += plan[res].dist * len(max_intersect)

                # delete partitons in lists
                for p in max_intersect:
                    if p in ls_pt:
                        ls_pt.remove(p)

        return cluster_to_partitions

    def map_src_nodes(self, geo_plan, ls_src_node):
        for sn in ls_src_node:
            if geo_plan[sn.net_node_name].pns:
                geo_plan[sn.net_node_name].pns.append(sn)
            else:
                geo_plan[sn.net_node_name].pns = [sn]

    def insert_dict_from_list(self, par2pnodes, ls_of_pnodes):
        for pnode in ls_of_pnodes:
            assert len(pnode.prev_nodes) == 1
            par = pnode.prev_nodes[0]
            if par in par2pnodes:
                par2pnodes[par].append(pnode)
            else:
                par2pnodes[par] = [pnode]

    def insert_dict_from_dict(self, par2pnodes, _par2pnodes):
        for par in _par2pnodes:
            if par in par2pnodes:
                par2pnodes[par].extend(_par2pnodes[par])
            else:
                par2pnodes[par] = _par2pnodes[par]

    def map_phy_nodes_to_clusters(self, root, network, geo_plan, clusters_to_partitions, phy_plan):
        # if root in clusters_to_partitions:
        #     # assert geo_plan[root].pns is not None
        #     return geo_plan[root].pns
        phy_nodes = dict()
        if geo_plan[root].pns is None:
            geo_plan[root].pns = []
        self.insert_dict_from_list(phy_nodes, geo_plan[root].pns)
        # phy_nodes = geo_plan[root].pns
        for child in geo_plan[root].children:
            phy_nodes_from_children = self.map_phy_nodes_to_clusters(child, network, geo_plan, clusters_to_partitions, phy_plan)
            # assert len(phy_nodes_from_children) > 0
            # phy_nodes.extend(phy_nodes_from_children)
            self.insert_dict_from_dict(phy_nodes, phy_nodes_from_children)
        if len(phy_nodes) == 0:
            return dict()
        # assert len(phy_nodes) > 0
        # if len(geo_plan[root].children) == 1:
        #     # assert geo_plan[root].pns is None
        #     return phy_nodes
        print "******************phy_nodes:", "\n".join(map(lambda x: str(x), phy_nodes))
        npn = dict()
        for par in phy_nodes:
            if len(phy_nodes[par]) == 1:
                npn[par] = phy_nodes[par]
                continue
            par_phy_node = phy_plan.get_parent(phy_nodes[par], root)
            # assert len(geo_plan[root].pns) == 0
            if par_phy_node == None:
                npn[par] = phy_nodes[par]
            else:
                if geo_plan[root].pns is None:
                    geo_plan[root].pns = [par_phy_node]
                else:
                    geo_plan[root].pns.append(par_phy_node)

                np = par_phy_node.prev_nodes[0]
                # assert np in phy_nodes
                npn[np] = [par_phy_node]
                # if np in phy_nodes:
                #     npn[np] = []
                #     for pn in phy_nodes[np]:
                #         if
                #         npn[np] = phy_nodes[np]
                #     npn[np].append(par_phy_node)
                # else:
                #     npn[np] = [par_phy_node]

        return npn


    def update_weights(self, network, clusters_to_datasize):
        for c in clusters_to_datasize:
            datasize = cluster_to_partitions[c]
            self.bfs(network, c, datasize)

        for src in network.links:
            for dest in network.links[src]:
                network.links[src][dest]["weight"] = network.links[src][dest]["weight"]/network.links[src][dest]["bandwidth"]

    def bfs(self, network, src, datasize):
        q = []
        visited = []

        q.append(src)

        while len(q)>0:
            cur = q.pop()
            visited.append(cur)
            for n in network.links[cur]:
                if n not in visited:
                    q.append(n)
                    network.links[cur][n]["weight"] += datasize

