import unittest
from src.utils import readJson
from src.network import Flow, CoFlow
from src.network import NetGraph
from src.scheduler.remain_flow_scheduler import RemainFlowScheduler


def make_net_graph(data_name='simple'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class RemainFlowSchedulerTest(unittest.TestCase):
    # Net graph:
    #
    #     2
    # 20 / \ 10
    #   /   \
    #  1     4
    #   \   /
    # 10 \ / 20
    #     3
    #
    #

    def setUp(self):
        self.net_graph = make_net_graph()
        self.flow_data = [[(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                          [(('n1', 'n2'), 10), (('n3', 'n4'), 95)]]
        coflow_dict = dict()
        for i in range(len(self.flow_data)):
            coflow_dict[i] = CoFlow(volume=sum(self.flow_data[i][j][1] for j in range(len(self.flow_data[i]))),
                                    start_end_volume_list=self.flow_data[i],
                                    id=i)
        self.scheduler = RemainFlowScheduler(net_graph=self.net_graph, coflow_dict=coflow_dict)

    def test_schedule(self):
        self.scheduler.schedule()
        self.assertEqual(1, len(self.scheduler.scheduled_coflow_dict))
        self.assertEqual(1, len(self.scheduler.scheduled_flow_dict))
