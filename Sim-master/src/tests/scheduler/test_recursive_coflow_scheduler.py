import unittest
from src.utils import readJson
from src.network import Flow, CoFlow
from src.network import NetGraph
from src.scheduler.recursive_coflow_scheduler import RecursiveCoflowScheduler


def make_net_graph(data_name='simple'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class RecursiveCoflowSchedulerTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph()
        self.flow_datas = [[(('n1', 'n2'), 80)],
                           [(('n1', 'n2'), 30)],
                           [(('n3', 'n4'), 100)],
                           [(('n1', 'n2'), 30)]]
        coflow_dict = dict()
        for i in range(len(self.flow_datas)):
            coflow_dict[i] = CoFlow(volume=self.flow_datas[i][0][1], start_end_volume_list=self.flow_datas[i], id=i)
        self.scheduler = RecursiveCoflowScheduler(net_graph=self.net_graph, coflow_dict=coflow_dict)

    def tearDown(self):
        pass

    def test_schedule(self):
        self.scheduler.schedule()
        self.assertEqual(2, len(self.scheduler.scheduled_coflow_dict))
        self.assertEqual(2, len(self.scheduler.unscheduled_coflow_dict))


if __name__ == '__main__':
    unittest.main()