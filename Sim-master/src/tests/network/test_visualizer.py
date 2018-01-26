import unittest
import networkx as nx
from src.network import Visualizer
import matplotlib.pyplot as plt


class VisualizerTest(unittest.TestCase):
    def setUp(self):
        self.nodes_kv = {}
        self.links_kv = {}
        self.visualizer = None

    def test_viz_simple(self):
        print "\ntest simple visualizer"
        self.nodes_kv = {
            "n1": {"pos": (0, 3), "power": 0.9},
            "n2": {"pos": (2, 5), "power": 0.6},
            "n3": {"pos": (2, 1), "power": 0.3},
            "n4": {"pos": (4, 3), "power": 1}
        }
        self.links_kv = {
            ("n1", "n2"): 20,
            ("n1", "n3"): 40,
            ("n2", "n3"): 20,
            ("n2", "n4"): 40,
            ("n3", "n4"): 20
        }
        self.visualizer = Visualizer(links_kv=self.links_kv, nodes_kv=self.nodes_kv)
        self.visualizer.visualize_network('../../../results/visualizer/simple.png')

    def test_GML(self):
        self.visualizer = Visualizer(gml_filename='../../../data/gml/AttMpls.gml')
        self.visualizer.visualize_network('../../../results/visualizer/AttMpls.png')
