import unittest

from src.mmcf import mmcf_optimize
from src.network.flow import CoFlow
from src.network.net_graph import NetGraph
from src.utils import readJson


class MinimizeCCTEasyTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('net_data.json')
        test_data = test_file['easy']
        self.net_graph = NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])
        flow_list = [(('n1', 'n2'), 10)]
        self.coflow = CoFlow(volume=10, start_end_volume_list=flow_list, id=0)

    def test_easy(self):
        print "\n@@@ test easy 1"
        cct, flow_kv = mmcf_optimize(self.coflow, self.net_graph)
        print "test 1 finished with cct %f." % cct


class MinimizeCCTSimpleTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('net_data.json')
        test_data = test_file['simple']
        self.net_graph = NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])
        self.coflow = CoFlow(volume=120, data=test_data, id=0)
        self.flow_kv = {
            1: {
                ('n1', 'n2'): 20,
                ('n2', 'n4'): 20,
            },
            2: {
                ('n1', 'n3'): 40,
                ('n3', 'n4'): 20,
                ('n3', 'n2'): 20,
                ('n2', 'n4'): 20,
            }
        }
        self.flow_kv_2 = {
            1: {
                ('n1', 'n2'): 40,
                ('n2', 'n3'): 40,
                ('n3', 'n4'): 40,
            },
            2: {
                ('n1', 'n2'): 80,
                ('n3', 'n2'): 80,
                ('n2', 'n4'): 80,
            }
        }

    def test_simple(self):
        print "\n@@@ test simple 1"
        cct, flow_kv = mmcf_optimize(self.coflow, self.net_graph)
        self.assertEqual(2, cct)
        # self.assertEqual(self.flow_kv, flow_kv)
        print "test 1 finished with cct %f." % cct


class MinimizeCCTRapierTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('net_data.json')
        test_data = test_file['rapier']
        self.net_graph = NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])
        self.coflow = CoFlow(volume=220, data=test_data, id=0)
        self.flow_kv = {
            1: {
                ('n1', 'n2'): 100,
                ('n2', 'n5'): 100,
            },
            2: {
                ('n1', 'n3'): 60,
                ('n3', 'n5'): 60,
            },
            3: {
                ('n1', 'n4'): 40,
                ('n4', 'n5'): 40,
            }
        }

    def test_simple(self):
        print "\n@@@ test simple 1"
        cct, flow_kv = mmcf_optimize(self.coflow, self.net_graph)
        # self.assertEqual(1.33, cct)
        # self.assertEqual(self.flow_kv, flow_kv)
        print(flow_kv)
        print "test 1 finished with cct %f." % cct


class MinimizeCCTB4Test(unittest.TestCase):
    def setUp(self):
        test_file = readJson('net_data.json')
        test_data = test_file['B4']
        self.net_graph = NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])
        self.coflow = CoFlow(volume=2000, data=test_data, id=0)
        self.flow_kv = {}
        self.coflow.print_flow_dict()

    def test_simple(self):
        print "\n@@@ test simple 1"
        cct, flow_kv = mmcf_optimize(self.coflow, self.net_graph)
        # self.assertEqual(1.33, cct)
        # self.assertEqual(self.flow_kv, flow_kv)
        print(flow_kv)
        print "test 1 finished with cct %f." % cct


class BidirectionalLinkTest(unittest.TestCase):
    def setUp(self):
        test_net_graph = {
            "n1": {"n2": 20.0, "n3": 40},
            "n2": {"n1": 20.0, "n4": 40.0, "n3": 20.0},
            "n3": {"n1": 40.0, "n4": 20.0, "n2": 20.0},
            "n4": {"n2": 40.0, "n3": 20.0}
        }
        test_net_node = {
            "n1": {"compPower": 1, "dataSources": ["p1", "p2"]},
            "n2": {"compPower": 1, "dataSources": ["p3"]},
            "n3": {"compPower": 1, "dataSources": []},
            "n4": {"compPower": 1, "dataSources": []}
        }
        test_flow_list = [
            (("n1", "n4"), 20.0),
            (("n1", "n4"), 40.0),
            (("n2", "n3"), 10.0)]
        self.net_graph = NetGraph(linkData=test_net_graph, nodesData=test_net_node)
        self.coflow = CoFlow(volume=70, start_end_volume_list=test_flow_list, id=0)
        self.flow_kv = {}

    def test_simple(self):
        print "\n@@@ test bidirectional 1"
        cct, flow_kv = mmcf_optimize(self.coflow, self.net_graph)
        # self.assertEqual(1.33, cct)
        # self.assertEqual(self.flow_kv, flow_kv)
        print(flow_kv)
        print "test 1 finished with cct %f." % cct

if __name__ == '__main__':
    unittest.main()
