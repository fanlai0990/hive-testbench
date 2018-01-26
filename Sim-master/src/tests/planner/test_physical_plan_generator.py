# -*- coding: utf-8 -*-
import unittest
from src.utils import readJson
from src.planner import PhysicalPlan

class SimplePlanGenTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('plan_data.json')
        data = test_file['PlanGen']
        self.physical_plan = PhysicalPlan(ref_node_map=data['QueryPlan'],
                                source_map=data['DataSource'],
                                source2net_map=data['Source2Net'],
                                destination_node=data['DataDestination'])
        self.physical_plan.build()

    def test_combine(self):
        self.physical_plan.build()
        print str(self.physical_plan)

if __name__ == '__main__':
    unittest.main()