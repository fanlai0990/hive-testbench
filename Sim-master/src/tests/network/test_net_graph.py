import unittest
import networkx as nx
from src.network import NetGraph


class NetGraphTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = None

    def test_read_gml(self):
        self.net_graph = NetGraph(gmlfilename='./net_data.gml')
        for key, bandwidth in self.net_graph.link_dict.get_link_kv().items():
            print key, bandwidth
        print [key for key in self.net_graph.nodes]
        self.net_graph.visualize('./../../../results/visualizer/simple.png')

        self.net_graph = NetGraph(gmlfilename='./../../../data/gml/AttMpls.gml')
        for key, bandwidth in self.net_graph.link_dict.get_link_kv().items():
            print key, bandwidth
        print [key for key in self.net_graph.nodes]
