import unittest
from src.network.link import Link, LinkDict, BidirectionalLinkDict


class LinkDictTest(unittest.TestCase):
    def setUp(self):
        self.link1 = Link(100, 'n1', 'n2')
        self.link2 = Link(20, 'n2', 'n3')
        self.link3 = Link(100, 'n2', 'n1')
        self.link4 = Link(20, 'n3', 'n2')
        self.link_dict = LinkDict()

    def tearDown(self):
        pass

    def test_is_duplicate(self):
        self.link_dict.add(self.link1)
        self.assertTrue(self.link_dict.is_duplicate(self.link1))
        self.assertTrue(self.link_dict.is_duplicate(self.link3))

    def test_add(self):
        # Add the same link for twice
        self.link_dict.add(self.link1)
        self.link_dict.add(self.link1)
        self.assertEqual(1, len(self.link_dict))
        # Add the reversed ordered start_end link
        self.link_dict.add(self.link3)
        self.assertEqual(1, len(self.link_dict))
        # Add second link
        self.link_dict.add(self.link2)
        self.assertEqual(2, len(self.link_dict))
        # Duplicated insertion test
        self.link_dict.add(self.link2)
        self.link_dict.add(self.link4)
        self.assertEqual(2, len(self.link_dict))

    def test_invalid_link(self):
        self.link_dict.add(self.link1)
        self.link_dict.add(self.link2)
        self.assertEquals(None, self.link_dict.get(start_node_id='n1', end_node_id='n3'))


class BidirectionalLinkDictTest(unittest.TestCase):
    def setUp(self):
        self.link1 = Link(100, 'n1', 'n2')
        self.link2 = Link(20, 'n2', 'n3')
        self.link3 = Link(100, 'n2', 'n1')
        self.link4 = Link(20, 'n3', 'n2')
        self.link_dict = BidirectionalLinkDict()

    def tearDown(self):
        pass

    def test_is_duplicate(self):
        self.link_dict.add(self.link1)
        self.assertTrue(self.link_dict.is_duplicate(self.link1))
        self.assertFalse(self.link_dict.is_duplicate(self.link3))

    def test_add(self):
        # Add the same link for twice
        self.link_dict.add(self.link1)
        self.link_dict.add(self.link1)
        self.assertEqual(1, len(self.link_dict))
        # Add the reversed ordered start_end link
        self.link_dict.add(self.link3)
        self.assertEqual(2, len(self.link_dict))
        # Add second link
        self.link_dict.add(self.link2)
        self.assertEqual(3, len(self.link_dict))
        # Duplicated insertion test
        self.link_dict.add(self.link2)
        self.link_dict.add(self.link4)
        self.assertEqual(4, len(self.link_dict))

    def test_invalid_link(self):
        self.link_dict.add(self.link1)
        self.link_dict.add(self.link2)
        self.assertEquals(None, self.link_dict.get(start_node_id='n1', end_node_id='n3'))

if __name__ == '__main__':
    unittest.main()
