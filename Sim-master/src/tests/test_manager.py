import unittest
from src.manager import (SimulationManager, EventManager, Manager,
                         SparkEnableSimulationManager, BaselineSparkSimulationManager)
from src.scheduler import (RecursiveCoflowScheduler, RemainFlowScheduler,
                          RecursiveRemainScheduler, CombineCoflowScheduler, MaxMinFlowScheduler)


class SimulationManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = SimulationManager(gml_path='network/net_data.gml',
                                         coflow_path='coflow_data.json',
                                         scheduler_class=RecursiveRemainScheduler)
                                         # scheduler_class=MaxMinFlowScheduler)

    def test_simulation(self):
        self.manager.print_all_coflows()
        # self.manager.visualize_netgraph()
        self.manager.simulate()


class SparkManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = SparkEnableSimulationManager(gml_path='../../data/gml/paper.gml',
                                                    spark_path='../../data/job/paper.txt',
                                                    scheduler_class=RecursiveRemainScheduler)

    def test_simulation_with_netnode1(self):
        self.manager.print_all_coflows()
        self.manager.simulate()


class BaselineManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = BaselineSparkSimulationManager(gml_path='../../data/gml/paper.gml',
                                                      spark_path='../../data/job/paper.txt',)

    def test_simulation_with_netnode1(self):
        self.manager.print_all_coflows()
        self.manager.simulate()

if __name__ == '__main__':
    unittest.main()
