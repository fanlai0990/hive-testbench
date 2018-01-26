import unittest
from src.utils import readJson
from src.network import Flow, CoFlow
from src.network import NetGraph


def make_net_graph(data_name='simple'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class FlowTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph(data_name='debug')
        self.link_kv = {
            ('n1', 'n2'): 10,
            ('n1', 'n3'): 10,
            ('n2', 'n4'): 10,
            ('n3', 'n4'): 10,
        }
        self.link_kv_line_diverge = {
            ('n0', 'n1'): 3.33,
            ('n1', 'n2'): 1.67,
            ('n1', 'n3'): 1.67,
            ('n2', 'n4'): 1.67,
            ('n3', 'n4'): 1.67
        }
        self.link_kv_diverge_line = {
            ('n1', 'n4'): 9.99,
            ('n1', 'n2'): 0.01,
            ('n2', 'n4'): 0.01,
            ('n4', 'n5'): round(10.00, 2)
        }
        self.link_kv_line_diverge_line = {
            ('n0', 'n1'): 10.0,
            ('n1', 'n4'): 9.99,
            ('n1', 'n3'): 0.01,
            ('n3', 'n4'): 0.01,
            ('n4', 'n5'): 10.00
        }

    def tearDown(self):
        pass

    def get_bandwidth(self, start, end):
        link = self.net_graph.link_dict.get(start_node_id=start, end_node_id=end)
        return link.bandwidth

    def test_simple_find_paths(self):
        flow = Flow(source_nid='n1', sink_nid='n4', volume=10, id=1)
        flow.make(net_graph=self.net_graph)
        self.assertEqual(['n1', 'n2', 'n4'], flow.node_lists[0])
        self.assertEqual(['n1', 'n3', 'n4'], flow.node_lists[1])
        self.assertEqual(10, self.get_bandwidth('n1', 'n2'))
        self.assertEqual(0, self.get_bandwidth('n2', 'n4'))
        self.assertEqual(0, self.get_bandwidth('n1', 'n3'))
        self.assertEqual(10, self.get_bandwidth('n3', 'n4'))

    def test_find_pathway_with_link_allocation_line_diverge(self):
        flow = Flow(source_nid='n0', sink_nid='n4', volume=3.33, id=1)
        flow.make(net_graph=self.net_graph, link_kv=self.link_kv_line_diverge)

    def test_find_pathway_with_link_allocation_diverge_line(self):
        flow = Flow(source_nid='n1', sink_nid='n5', volume=10.0, id=1)
        flow.make(net_graph=self.net_graph, link_kv=self.link_kv_diverge_line)
        self.assertEqual(19.99, self.get_bandwidth("n1", "n2"))
        self.assertEqual(9.99, self.get_bandwidth("n2", "n4"))
        self.assertEqual(0.01, self.get_bandwidth("n1", "n4"))
        self.assertEqual(30.0, self.get_bandwidth("n4", "n5"))

    def test_find_pathway_with_link_allocation_line_diverge_line(self):
        flow = Flow(source_nid='n0', sink_nid='n5', volume=10.0, id=1)
        flow.make(net_graph=self.net_graph, link_kv=self.link_kv_line_diverge_line)

    def test_release(self):
        flow = Flow(source_nid='n1', sink_nid='n4', volume=10, id=1)
        flow.make(net_graph=self.net_graph, link_kv=self.link_kv)
        flow.release()
        self.assertEqual(20, self.get_bandwidth('n1', 'n2'))
        self.assertEqual(10, self.get_bandwidth('n2', 'n4'))
        self.assertEqual(10, self.get_bandwidth('n1', 'n3'))
        self.assertEqual(20, self.get_bandwidth('n3', 'n4'))
        self.assertEqual(0, flow.bandwidth_sum)


class CoFlowTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph()
        self.flow_data = [(('n1', 'n4'), 100), (('n1', 'n4'), 60)]

    def tearDown(self):
        pass

    def test_coflow_init(self):
        coflow = CoFlow(volume=160, start_end_volume_list=self.flow_data, id=1)

if __name__ == '__main__':
    unittest.main()
