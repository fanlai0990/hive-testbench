import unittest

from src.network.path import Path, Link
from src.rapier import minimize_coflow_completion_time


class MinimizeCCTSimpleTest(unittest.TestCase):
    def setUp(self):
        link1 = Path([Link(100, 'n1', 'n2')])
        link2 = Path([Link(40, 'n1', 'n2')])
        link3 = Path([Link(60, 'n1', 'n2')])
        links = [link1, link2, link3]
        self.coflow_kv_1 = {
            1: (links[1:3], 40),
            2: (links[1:3], 60),
        }
        self.coflow_kv_2 = {
            1: (links[:], 40),
            2: (links[:], 80),
            3: (links[:], 100)
        }

    def test_simple_1(self):
        print "\n@@@ test simple 1"
        cct = minimize_coflow_completion_time(self.coflow_kv_1)
        print "test 1 finished with cct %f." % cct

    def test_simple_2(self):
        print "\n@@@ test simple 2"
        cct = minimize_coflow_completion_time(self.coflow_kv_2)
        print "test 2 finished with cct %f." % cct


if __name__ == '__main__':
    unittest.main()
