import unittest
from src.network.path import Path
from src.network.link import Link


class PathTest(unittest.TestCase):
    def setUp(self):
        self.link1 = Link(100, 'n1', 'n2')
        self.link2 = Link(20, 'n2', 'n3')
        self.link3 = Link(30, 'n3', 'n4')
        self.link4 = Link(100, 'n4', 'n2')
        self.links = [self.link1, self.link2, self.link3]

    def tearDown(self):
        pass

    def check_find_path_with_ordered_edges(self, edges):
        link = Path(edges)
        self.assertEqual(link.bandwidth, 20)
        for expected, reality in zip(['n1', 'n2', 'n3'], link.pathway.node_list):
            self.assertEqual(reality, expected)

    def test_find_path_with_ordered_edges_successful(self):
        self.check_find_path_with_ordered_edges(self.links)

    def test_find_path_with_ordered_edges_link_swap_test(self):
        map(lambda x: x.swap_ids, self.links)
        self.check_find_path_with_ordered_edges(self.links)

    def test_find_path_with_ordered_edges_failure(self):
        try:
            self.links.append(self.link1)
            _ = Path(self.links)
        except AssertionError as e:
            self.assertEqual(str(e), 'Found link not connected')

    def test_find_path_with_ordered_edges_cycle(self):
        self.links.append(self.link4)
        try:
            _ = Path(self.links)
        except AssertionError as e:
            self.assertEqual(str(e), 'Found cycle')

if __name__ == '__main__':
    unittest.main()
