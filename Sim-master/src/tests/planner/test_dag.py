# -*- coding: utf-8 -*-
import unittest
from src.planner import DAGNode, DAG

# global variable helps testing
g_sum_in_stream = 0

def double_connect_nodes(upper_node, lower_node):
    upper_node.insert_sub_node(lower_node)
    lower_node.insert_prev_node(upper_node)

class DagNodeTest(unittest.TestCase):
    def setUp(self):
        self.root = DAGNode(type="ROOT",name="tr1",in_stream=1,out_stream=1)
        self.sub1 = DAGNode(type="ROOT", name="sb1",in_stream=1,out_stream=1)
        self.prev1 = DAGNode(type="ROOT", name="pv1",in_stream=1,out_stream=1,
                             fanout=2)
        double_connect_nodes(self.root, self.sub1)
        double_connect_nodes(self.prev1, self.root)

    def tearDown(self):
        del self.root
        del self.sub1
        del self.prev1

    def test_simple(self):
        self.assertEqual(str(self.root),
                "name:tr1, type:ROOT, in:1.000000, out:1.000000, parent:[pv1]")

    def test_flow_data(self):
        self.root.out_stream = None
        self.root.in_stream = None
        self.prev1.in_stream = None
        sub2 = DAGNode(type="ROOT", name="sub2",in_stream=2,out_stream=2)
        double_connect_nodes(self.root, sub2)
        self.root.flow_data()
        self.prev1.flow_data()
        self.assertEqual(self.root.out_stream, 3.0)
        self.assertEqual(self.prev1.in_stream, 3.0)
        self.assertEqual(self.prev1.out_stream, 6.0)

class DagTest(unittest.TestCase):

    def setUp(self):
        root = DAGNode(type="ROOT",name="r1")
        self.test_dag = DAG(root=root)
        sub1 = DAGNode(type="ROOT",name="l1_1")
        sub2 = DAGNode(type="ROOT",name="l1_2")
        sub11 = DAGNode(type="ROOT",name="l2_1",in_stream=1)
        sub12 = DAGNode(type="ROOT", name="l2_2", in_stream=2)
        sub21 = DAGNode(type="ROOT", name="l2_3", in_stream=3)
        double_connect_nodes(root, sub1)
        double_connect_nodes(root, sub2)
        double_connect_nodes(sub1, sub11)
        double_connect_nodes(sub1, sub12)
        double_connect_nodes(sub2, sub21)

    def tearDown(self):
        pass

    def test_top_sort(self):
        expected_top_node_names = ["r1", "l1_2", "l2_3", "l1_1", "l2_2", "l2_1"]
        top_node_names = map(lambda x: x.name, self.test_dag.top_nodes)
        self.assertEqual(len(expected_top_node_names), len(top_node_names))
        for i in xrange(0, len(top_node_names)):
            self.assertEqual(expected_top_node_names[i], top_node_names[i])

    def test_build(self):
        self.test_dag.build()
        self.assertEqual(self.test_dag.root.out_stream, 6)

    def test_str_simple(self):
        self.test_dag.build()
        print "test dag:\n-------------------------\n %s" %str(self.test_dag)
        str_top_nodes = map(lambda x: x.name, self.test_dag.top_nodes)
        print "test dag's top nodes: %s" %','.join(str_top_nodes)

    def test_recursive_dispatcher(self):
        self.test_dag.build()
        def print_in_stream(node):
            global g_sum_in_stream
            g_sum_in_stream += node.in_stream
        self.test_dag.recursive_from_root(print_in_stream)
        self.assertEqual(g_sum_in_stream, 18)

    def test_leaves(self):
        self.assertEqual(len(self.test_dag.leaves), 3)

if __name__ == '__main__':
    unittest.main()

