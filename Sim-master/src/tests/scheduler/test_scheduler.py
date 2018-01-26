import unittest
from src.utils import readJson
from src.network import Flow, CoFlow
from src.network import NetGraph
from src.scheduler.scheduler import Scheduler


def make_net_graph(data_name='simple'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph()
        self.flow_datas = [[(('n1', 'n2'), 100)], [(('n1', 'n2'), 80)], [(('n1', 'n2'), 30)]]
        coflow_dict = dict()
        for i in range(len(self.flow_datas)):
            coflow_dict[i] = CoFlow(volume=self.flow_datas[i][0][1], start_end_volume_list=self.flow_datas[i], id=i)
        self.scheduler = Scheduler(net_graph=self.net_graph, coflow_dict=coflow_dict)

    def tearDown(self):
        pass

    def test_coflow_sort(self):
        self.scheduler.sort_coflows()
        fid_order = [2, 1, 0]
        for i in range(len(self.scheduler.sorted_coflow)):
            self.assertEqual(fid_order[i], self.scheduler.sorted_coflow[i][0])

    def test_allocate_minimum(self):
        self.scheduler.sort_coflows()
        self.assertEqual(3, len(self.scheduler.unscheduled_coflow_dict))
        for i in range(3):
            self.scheduler.allocate_minimum()
            self.assertEqual(i + 1, len(self.scheduler.scheduled_coflow_dict))
            for _, coflow in self.scheduler.scheduled_coflow_dict.items():
                coflow.release()
            self.scheduler.sort_coflows()
        self.assertEqual(0, len(self.scheduler.unscheduled_coflow_dict))
        self.assertEqual(3, len(self.scheduler.scheduled_coflow_dict))

    def test_remove_coflow(self):
        for fid in (0, 1, 2):
            self.scheduler.remove_coflow(id=fid)
        self.assertEqual(0, len(self.scheduler.unscheduled_coflow_dict))
        self.assertEqual(0, len(self.scheduler.scheduled_coflow_dict))


if __name__ == '__main__':
    unittest.main()