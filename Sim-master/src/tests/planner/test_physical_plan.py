# -*- coding: utf-8 -*-
import unittest
from src.utils import readJson
from src.planner import LogicalPlan
from src.planner import PhysicalPlan


# helper funcitons
def get_name2node_map(pnodes):
    name2node_map = {}
    for pnode in pnodes:
        name2node_map[pnode.name] = pnode
    return name2node_map

class SimplePhysicalPlanTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('plan_data.json')
        self.data = test_file['simple']
        logical_plan = LogicalPlan(ref_node_map=self.data['QueryPlan'],
                                        source_map=self.data['DataSource'])
        logical_plan.build()
        self.physical_plan = PhysicalPlan(logical_plan=logical_plan,
                                        source2net_map=self.data['Source2Net'])

    def tearDown(self):
        pass

    def test_build(self):
        self.physical_plan.build()
        self.physical_plan.geo_build()
        print str(self.physical_plan)

    def test_partitions(self):
        self.physical_plan.build()
        self.assertEqual(set(self.physical_plan.get_partitions()),
                         set(self.data['DataSource']))

    def test_copy_node(self):
        pass


class PhysicalPlanTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('plan_data.json')
        self.data = test_file['complex']
        self.physical_plan = PhysicalPlan(ref_node_map=self.data['QueryPlan'],
                                source_map=self.data['DataSource'],
                                destination_node=self.data['DataDestination'])

    def test_build(self):
        self.assertFalse(self.physical_plan.is_built)
        self.physical_plan.build()
        self.assertTrue(self.physical_plan.is_built)
        print str(self.physical_plan)

    def test_geo_build(self):
        self.assertFalse(self.physical_plan.is_built)
        self.physical_plan.build()
        self.assertTrue(self.physical_plan.is_built)
        self.assertFalse(self.physical_plan.is_geo_built)
        self.physical_plan.geo_build(source2net_map=self.data['Source2Net'])
        self.assertTrue(self.physical_plan.is_geo_built)
        print str(self.physical_plan)


    def test_source_group_nodes(self):
        self.physical_plan.build()
        source_node_lists = self.physical_plan.source_group_nodes()
        print source_node_lists

class YASimplePhysicalPlanTest(unittest.TestCase):
    '''
    Yet Another physical plan test
    '''
    def setUp(self):
        test_file = readJson('plan_data.json')
        self.data = test_file['ya_simple']
        self.physical_plan = PhysicalPlan(ref_node_map=self.data['QueryPlan'],
                                source_map=self.data['DataSource'],
                                source2net_map=self.data['Source2Net'],
                                destination_node=self.data['DataDestination'])

    def test_build(self):
        self.physical_plan.build()
        print str(self.physical_plan)

    def test_geo_build(self):
        self.physical_plan.build()
        self.physical_plan.geo_build()
        print str(self.physical_plan)

class COMBINEPhysicalPlanTest(unittest.TestCase):
    '''
    Yet Another physical plan test
    '''
    def setUp(self):
        test_file = readJson('plan_data.json')
        self.data = test_file['combine1']
        self.physical_plan = PhysicalPlan(ref_node_map=self.data['QueryPlan'],
                                source_map=self.data['DataSource'],
                                source2net_map=self.data['Source2Net'],
                                destination_node=self.data['DataDestination'])

    def test_build(self):
        self.physical_plan.build()
        print str(self.physical_plan)

    def test_geo_build(self):
        self.physical_plan.build()
        self.physical_plan.geo_build()
        print str(self.physical_plan)

    def test_get_parent(self):
        self.physical_plan.build()
        self.physical_plan.geo_build()
        self.physical_plan.update()
        print str(self.physical_plan)
        print "++++++++++++++++++++++++++++++++++++++"
        name2node = get_name2node_map(self.physical_plan.top_nodes)
        pnodes = [name2node['SOURCE_7'], name2node['SOURCE_8']]
        node_copy = self.physical_plan.get_parent(pnodes, "nx")
        print str(node_copy)
        print str(self.physical_plan)

class Query3PhysicalPlanTest(unittest.TestCase):
    def setUp(self):
        test_file = readJson('plan_data.json')
        data = test_file['query3']
        self.physical_plan = PhysicalPlan(ref_node_map=data['QueryPlan'],
                                    source_map=data['DataSource'],
                                    source2net_map=data['Source2Net'],
                                    destination_node=data['DataDestination'])

    def test_geo_build(self):
        self.physical_plan.build()
        self.physical_plan.geo_build()
        print str(self.physical_plan)

if __name__ == '__main__':
    unittest.main()
