import unittest

from src.mmcf import maxflow_optimize
from src.network.flow import CoFlow
from src.network.net_graph import NetGraph


class BidirectionalLinkTest(unittest.TestCase):
    def setUp(self):
        test_net_graph = {
            "n1": {"n2": 20.0, "n3": 10},
            "n2": {"n1": 20.0, "n3": 20.0, "n4": 0.0},
            "n3": {"n1": 40.0, "n2": 20.0, "n4": 0.0},
            "n4": {"n2": 40.0, "n3": 20.0}
        }
        test_net_node = {
            "n1": {"compPower": 1, "dataSources": ["p1", "p2"]},
            "n2": {"compPower": 1, "dataSources": ["p3"]},
            "n3": {"compPower": 1, "dataSources": []},
            "n4": {"compPower": 1, "dataSources": []}
        }
        test_flow_list = [
            (("n1", "n4"), 100.0),
            (("n1", "n4"), 100.0),
            (("n2", "n3"), 100.0)]
        self.net_graph = NetGraph(linkData=test_net_graph, nodesData=test_net_node)
        self.coflow = CoFlow(volume=300, start_end_volume_list=test_flow_list, id=0)
        self.flow_kv = {}

    def test_simple(self):
        print "\n@@@ test bidirectional 1"
        flow_kv = maxflow_optimize(self.coflow, self.net_graph)
        # self.assertEqual(1.33, cct)
        # self.assertEqual(self.flow_kv, flow_kv)
        print(flow_kv)

if __name__ == '__main__':
    unittest.main()
