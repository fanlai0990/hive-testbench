from link import Link
from copy import deepcopy
import logging

# setup logger
logger = logging.getLogger(__name__)


class PathWay(object):
    def __init__(self, source, sink, bandwidth, edge_tuple=None, node_list=None):
        """
        :param edge_tuple: (node1, node2) directed edge
        :param bandwidth: the bandwidth should allocate at that edge
        """
        if node_list is None:
            assert len(edge_tuple) == 2
            self.node_list = list(edge_tuple)
        else:
            self.node_list = node_list
        self.bandwidth = bandwidth
        self.source = source
        self.sink = sink

    def append(self, edge_tuple, bandwidth):
        assert len(edge_tuple) == 2
        bandwidth = round(bandwidth, 2)
        # TODO: float point arithmatic problem, final solution: lpInt in optimizer, use decimal package in other codes
        if round(abs(round(self.bandwidth, 2) - bandwidth), 2) > 0.01 or self.node_list[-1] != edge_tuple[0] or self.complete:
            return False
        self.node_list.append(edge_tuple[1])
        return True

    @property
    def complete(self):
        return self.source is self.node_list[0] and self.sink is self.node_list[-1]

    @staticmethod
    def duplicate(pathway):
        new_pathway = deepcopy(pathway)
        return new_pathway


class Path(Link):
    def __init__(self, links, pathway=None):
        self.links = links
        if pathway is None:
            node_list, min_bandwidth = self.find_path_with_ordered_edges()
            self.pathway = PathWay(source=node_list[0], sink=node_list[-1], bandwidth=min_bandwidth, node_list=node_list)
        else:
            self.pathway = pathway
        super(Path, self).__init__(self.pathway.bandwidth,
                                             self.pathway.source,
                                             self.pathway.sink
                                             )

    def release(self, bandwidth=None):
        bandwidth = bandwidth or self.bandwidth
        for link in self.links:
            link.release(bandwidth)
        self.bandwidth -= bandwidth

    def find_path_with_ordered_edges(self, links=None):
        """
        Find out pathway given ordered edges.
         We define the edges is ordered if the prev link connect to next link.
        :param links: ordered edges
        :return: [nid1, nid2, nid3], minBandwidth
        """
        def check_cycle(nid_list, nid):
            assert (nid not in nid_list), 'Found cycle'
            nid_list.append(nid)

        links = links or self.links
        prev_link = links[0]
        pathway = [prev_link.start_node_id]
        min_bandwidth = prev_link.bandwidth
        for link in links[1:]:
            prev_link_ids = set(prev_link.ids)
            link_ids = set(link.ids)
            intersect_id_set = link_ids.intersection(prev_link_ids)
            assert(len(intersect_id_set)) == 1
            intersect_id = list(intersect_id_set)[0]
            if prev_link.end_node_id != intersect_id:
                prev_link.swap_ids()
            if link.start_node_id != intersect_id:
                link.swap_ids()
            assert(prev_link.end_node_id == link.start_node_id), 'Found link not connected'
            prev_link = link
            check_cycle(pathway, prev_link.start_node_id)
            min_bandwidth = min(prev_link.bandwidth, min_bandwidth)
        check_cycle(pathway, prev_link.end_node_id)
        return pathway, min_bandwidth

    def allocate(self, bandwidth=None):
        bandwidth_to_allocate = bandwidth or self.bandwidth
        self.bandwidth = round(bandwidth_to_allocate, 2)
        # check if such bw is ok for all links
        for link in self.links:
            bandwidth_to_allocate_on_link = link.check_bandwidth_assign(bandwidth_to_allocate)
            if bandwidth_to_allocate_on_link < self.bandwidth:
                self.bandwidth = bandwidth_to_allocate_on_link
        # final bandwidth assign for path
        for link in self.links:
            link.allocate(self.bandwidth)

        node_list_str = str()
        for node in self.pathway.node_list:
            node_list_str += str(node) + ' '
        print '\t' + str(self.bandwidth) + '\t' + node_list_str
        # logger.info('Try to allocate ' + str(self.bandwidth) + ' bandwidth on path\n' + node_list_str)

    def print_path(self):
        path_str = "Path info\n"
        path_str += "path length: " + str(len(self.links)) + " "
        for link in self.links:
            path_str += str(link)
        path_str += '\n'
        # print path_str.rstrip('\n')
        # logger.debug(path_str.rstrip('\n'))
