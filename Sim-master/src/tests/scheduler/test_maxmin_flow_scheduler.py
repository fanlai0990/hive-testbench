import unittest
from src.utils import readJson
from src.network import Flow
from src.network import NetGraph
from src.scheduler.maxmin_flow_scheduler import MaxMinFlowScheduler
from src.mmcf import mmcf_optimize


def make_net_graph(data_name='simple11'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class MaxMinFlowSchedulerTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph()
        self.flow_data = [(('n1', 'n4'), 10),
                           (('n1', 'n4'), 5),
                           (('n2', 'n4'), 5),
                           (('n1', 'n3'), 5),
                           (('n1', 'n2'), 5)]
        flow_dict = dict()
        for i in range(len(self.flow_data)):
            flow_dict[i] = Flow(source_nid=self.flow_data[i][0][0],
                                sink_nid=self.flow_data[i][0][1],
                                volume=self.flow_data[i][1],
                                id=i)
        self.scheduler = MaxMinFlowScheduler(net_graph=self.net_graph, flow_dict=flow_dict, optimizer=mmcf_optimize)

    def test_schedule(self):
        self.scheduler.schedule()


if __name__ == '__main__':
    unittest.main()
