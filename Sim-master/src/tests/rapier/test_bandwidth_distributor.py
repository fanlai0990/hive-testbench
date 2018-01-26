import unittest

from src.network.path import Path, Link
from src.rapier import distribute_bandwidth


class DistributeBandwidthTest(unittest.TestCase):
    def setUp(self):
        # link that has only one edge
        short_path1 = Path([Link(40, 'n1', 'n2')])
        short_path2 = Path([Link(60, 'n1', 'n2')])
        self.short_paths = [short_path1, short_path2]
        # link that has two edges
        long_path1 = Path([Link(40, 'n1', 'n2'), Link(40, 'n2', 'n3')])
        long_path2 = Path([Link(60, 'n1', 'n2'), Link(80, 'n2', 'n3')])
        self.long_paths = [long_path1, long_path2]
        # coflow definition
        self.coflow_kv_short_1 = {
            1: (self.short_paths[:], 40),
            2: (self.short_paths[:], 60),
        }
        self.coflow_kv_short_2 = {
            1: (self.short_paths[:], 40),
            2: (self.short_paths[:], 60),
            3: (self.short_paths[:], 80),
        }
        self.coflow_kv_short_3 = {
            1: (self.short_paths[:], 50),
            2: (self.short_paths[:], 20),
            3: (self.short_paths[:], 30),
        }
        self.coflow_kv_long = {
            1: (self.long_paths[:], 40),
            2: (self.long_paths[:], 80),
            3: (self.long_paths[:], 100)
        }

    def test_short_1(self):
        print "\n@@@ short link test 1"
        distribute_bandwidth([self.coflow_kv_short_1])
        show_remained_bandwidth(self.short_paths)

    def test_short_2(self):
        print "\n@@@ short link test 2"
        distribute_bandwidth([self.coflow_kv_short_2])
        show_remained_bandwidth(self.short_paths)

    def test_short_3(self):
        print "\n@@@ short link test 3"
        distribute_bandwidth([self.coflow_kv_short_3])
        show_remained_bandwidth(self.short_paths)

    def test_long(self):
        pass


# print out function
def show_remained_bandwidth(paths):
    path_num = 1
    for path in paths:
        print "link %d" % path_num
        link_num = 1
        for link in path.links:
            print "\tedge " + str(link_num) + " remained bandwidth: " + str(link.bandwidth)
            link_num += 1
        path_num += 1


if __name__ == '__main__':
    unittest.main()
