# -*- coding: utf-8 -*-
import unittest
from src.utils import readJson
from src.planner import LogicalNode, LogicalPlan

class LogicalPlanTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('plan_data.json')
        data = test_file['simple']
        self.logical_plan = LogicalPlan(ref_node_map=data['QueryPlan'],
                                        source_map=data['DataSource'])

    def tearDown(self):
        pass

    def test_build(self):
        self.logical_plan.build()
        print str(self.logical_plan)

if __name__ == '__main__':
    unittest.main()