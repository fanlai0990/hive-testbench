import unittest
from src.utils import readJson
from src.network import CoFlow
from src.network import NetGraph
from src.scheduler.combine_coflow_scheduler import CombineCoflowScheduler


def make_net_graph(data_name='simple2'):
    test_file = readJson('net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


class TestCombinedCoflowScheduler(unittest.TestCase):
    def setUp(self):
        self.net_graph = make_net_graph()

        self.flow_data_success = [
            [(('n1', 'n4'), 40)],
            [(('n2', 'n4'), 100)],
            [(('n2', 'n4'), 200)]
        ]
        self.coflow_dict_success = dict()
        for i in range(len(self.flow_data_success)):
            self.coflow_dict_success[i] = CoFlow(volume=sum(f[1] for f in self.flow_data_success[i]),
                                                 start_end_volume_list=self.flow_data_success[i],
                                                 id=i)
        self.flow_data_fail = [
            [(('n3', 'n4'), 100)],
            [(('n1', 'n2'), 80)],
            [(('n1', 'n2'), 30)]
        ]
        self.coflow_dict_fail = dict()
        for i in range(len(self.flow_data_fail)):
            self.coflow_dict_fail[i] = CoFlow(volume=self.flow_data_fail[i][0][1],
                                              start_end_volume_list=self.flow_data_fail[i],
                                              id=i)

    def test_combined_success(self):
        self.scheduler = CombineCoflowScheduler(net_graph=self.net_graph, coflow_dict=self.coflow_dict_success)
        print self.scheduler.schedule()

    def test_combined_fail(self):
        self.scheduler = CombineCoflowScheduler(net_graph=self.net_graph, coflow_dict=self.coflow_dict_fail)
        print self.scheduler.schedule()


if __name__ == '__main__':
    unittest.main()
