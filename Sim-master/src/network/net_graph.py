from net_node import NetNode
from visualizer import Visualizer
from link import Link, LinkDict, BidirectionalLinkDict
import networkx as nx
import src.globals


class NetGraph(object):
    def __init__(self, linkData=None, nodesData=None, gmlfilename=None, defaultbandwidth=1000, bidirectional=True):
        self.G = None
        self.APSP = None
        self.links = {}  # links: {node : {neighbor : {"bandwith" : bw, "status" : "free/busy", "available" : time}}}
        self.nodes = {}  # nodes: {"node" : NetNode}
        if bidirectional:
            self.link_dict = BidirectionalLinkDict()
        else:
            self.link_dict = LinkDict()

        if gmlfilename is None:
            for node, neighbors in linkData.items():
                self.links[node] = {}
                for nid, bw in neighbors.items():
                    self.links[node][nid] = {"bandwidth": bw, "status": "free", "available": 0.0, "weight": 0.0}
                    self.link_dict.add(Link(bandwidth=bw, start=node, end=nid))
            for key, value in nodesData.items():
                self.nodes[key] = NetNode(key, value)
        else:
            self.G = nx.read_gml(gmlfilename)
            for key, data in self.G.nodes(data=True):
                node_value_dict = {}
                for data_key, data_value in data.items():
                    node_value_dict[data_key] = data_value
                self.nodes[key] = NetNode(key, node_value_dict)
            for (start_node, end_node, link_labels_dict) in self.G.edges(data=True):
                # Leave some scratch bandwidth for SDN-based solution
                scratch_ratio = 0
                if not src.globals.isBaseline:
                    scratch_ratio = 0.1
                scale_factor = (1.0 - scratch_ratio)

                if 'bandwidth' in link_labels_dict:
                    self.link_dict.add(Link(start=start_node, end=end_node, bandwidth=int(scale_factor * link_labels_dict['bandwidth'])))
                    if bidirectional:
                        self.link_dict.add(Link(start=end_node, end=start_node, bandwidth=int(scale_factor * link_labels_dict['bandwidth'])))
                else:  # use the default value specified
                    self.link_dict.add(Link(start=start_node, end=end_node, bandwidth=int(scale_factor * defaultbandwidth)))
                    if bidirectional:
                        self.link_dict.add(Link(start=end_node, end=start_node, bandwidth=int(scale_factor * defaultbandwidth)))

            # Remember all pairs shortest paths
            self.APSP = nx.all_pairs_shortest_path(self.G)

    def print_network(self):
        print "Network Nodes:"
        for (key, value) in self.nodes.items():
            print key, value.partition

        print
        print "Network Topology:"
        for (node, neighbors) in self.links.items():
            print node, ": {",
            for (neighbor, value) in neighbors.items():
                print neighbor, ":", value["bandwidth"], ",",
            print "}"

    def visualize(self, graph_filename):
        viz = Visualizer(graph=self.G)
        viz.visualize_network(graph_filename)
