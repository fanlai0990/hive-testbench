from . import PathWay, Path
from src.exceptions import AllocationException
from copy import deepcopy
import logging

# setup logger
logger = logging.getLogger(__name__)


class FlowBase(object):
    def __init__(self, volume, id):
        self.volume = float(volume)
        self.id = id

    def make(self, net_graph):
        raise Exception('Not implemented')

    def release(self):
        raise Exception('Not Implemented')


class Flow(FlowBase):
    def __init__(self, source_nid, sink_nid, volume, id):
        super(Flow, self).__init__(volume=volume, id=id)
        self.total_volume = volume
        self.transmitted_volume = 0
        self.source_nid = source_nid
        self.sink_nid = sink_nid
        self.net_graph = None
        self.node_lists = None
        self.pathways = None
        self.paths = None
        self.bandwidth_sum = 0
        self.assigned_bw = 0        # used for max-min scheduler
        self.max_bw_path = None     # used for max-min scheduler

    def transmit(self, transmission_time=None, transmission_volume=None):
        transmission_volume = transmission_volume or float(self.assigned_bw) * transmission_time
        self.transmitted_volume += transmission_volume
        self.volume -= transmission_volume
        assert self.volume + self.transmitted_volume == self.total_volume

    def remain(self, remain_volume=None, remain_time=None, maxmin=False):
        if remain_volume is None:
            assert remain_time is not None
            if maxmin is True:
                remain_volume = remain_time * float(self.assigned_bw)
            else:
                remain_volume = remain_time * float(self.bandwidth_sum)
        if remain_volume < round(0.01, 2):
            self.volume = float(0.0)
            self.transmitted_volume = self.total_volume
        else:
            self.volume = remain_volume
            self.transmitted_volume = self.total_volume - remain_volume

    def __str__(self):
        return str(self.id) + ': { source: ' + self.source_nid + ',\tsink: ' + self.sink_nid + ',\tvolume: ' \
               + str(self.volume) + ' }'

    def make(self, net_graph, link_kv=None, coflow_id=None):
        if link_kv is None:
            # Try to find path with the simple find paths algorithm
            self.net_graph = net_graph
            self.node_lists = self.simple_find_paths()
            self.paths = Flow.make_paths(net_graph=self.net_graph, node_lists=self.node_lists)
        else:
            self.pathways = self.find_pathway_with_link_allocation(link_kv)
            self.paths = Flow.make_paths(net_graph=net_graph, pathways=self.pathways)

        if not self.paths:
            raise AllocationException("No path for flow to transmit")
        else:
            # print "Path bandwidth before allocation for flow " + str(self.id)
            logger.debug("Path bandwidth before allocation for flow " + str(self.id) + "\n")
            for path in self.paths:
                path.print_path()

            if coflow_id is not None:
                print "Flow allocate " + str(coflow_id)
            print '\t' + str(self.id) + '\t' + str(self.volume)
            logger.info('Bandwidth allocation for flow' + str(self.id))
            map(lambda x: x.allocate(), self.paths)
            self.bandwidth_sum = self.cal_bandwidth_sum()

            # print "path bandwidth after allocation for flow " + str(self.id)
            logger.debug("Path bandwidth after allocation for flow " + str(self.id) + "\n")
            for path in self.paths:
                path.print_path()

    def cal_bandwidth_sum(self):
        return reduce(lambda acc, path: acc + path.bandwidth, self.paths, 0)

    def find_pathway_with_link_allocation(self, link_kv):
        link_kv_info = "Link bandwidth allocation\n"
        for link_edge, link_bw in link_kv.items():
            link_kv_info += '(' + str(link_edge[0]) + ', ' + str(link_edge[1]) + '): ' + str(round(link_bw, 2)) + '\n'
        # print link_kv_info
        logger.info(link_kv_info.rstrip('\n'))

        pathways = []
        # Initialize the pathways
        for edge_tuple, edge_bandwidth in link_kv.items():
            if edge_tuple[0] == self.source_nid:
                if edge_bandwidth != 0:
                    pathways.append(PathWay(source=self.source_nid,
                                            sink=self.sink_nid,
                                            bandwidth=edge_bandwidth,
                                            edge_tuple=edge_tuple))
                link_kv.pop(edge_tuple)

        while len(link_kv) != 0:
            # print "link_kv length: ", len(link_kv)
            # print link_kv

            if_link_added = False
            for edge_tuple, edge_bandwidth in link_kv.items():
                if edge_bandwidth == 0:
                    link_kv.pop(edge_tuple)
                    continue
                for pathway in pathways:
                    if round(abs(pathway.bandwidth - edge_bandwidth), 2) < round(0.01, 2):  # one link @ one edge
                        if pathway.append(edge_tuple, round(edge_bandwidth, 2)):
                            link_kv.pop(edge_tuple)
                            if_link_added = True
                            break
                    elif round(pathway.bandwidth - edge_bandwidth, 2) >= round(0.01, 2):      # pathways diverge from same edge
                        new_pathway = PathWay.duplicate(pathway)
                        original_bandwidth = pathway.bandwidth
                        pathway.bandwidth = round(edge_bandwidth, 2)
                        if pathway.append(edge_tuple, round(edge_bandwidth, 2)):
                            link_kv.pop(edge_tuple)
                            new_pathway.bandwidth = round(round(new_pathway.bandwidth, 2) - round(edge_bandwidth, 2), 2)
                            pathways.append(new_pathway)
                            if_link_added = True
                            break
                        else:
                            pathway.bandwidth = original_bandwidth
                    elif round(pathway.bandwidth - edge_bandwidth, 2) <= round(-0.01, 2):     # pathways converge to same edge
                        if pathway.append(edge_tuple, round(pathway.bandwidth, 2)):
                            link_kv[edge_tuple] = round(round(link_kv[edge_tuple], 2) - round(pathway.bandwidth, 2), 2)
                            if_link_added = True
                            break

            if if_link_added is False:
                logger.debug('allocation not added to the path\n' + str(link_kv))
                break

        # filter out the paths where the end node does not match
        final_pathways = list()
        for pathway in pathways:
            if pathway.node_list[-1] == self.sink_nid:
                final_pathways.append(pathway)

        return final_pathways

    @staticmethod
    def find_links(net_graph, node_list):
        assert len(node_list) >= 2, 'path is less than 2'
        prev_nid = node_list[0]
        links = []
        for nid in node_list[1:]:
            links.append(net_graph.link_dict.get(start_node_id=prev_nid, end_node_id=nid))
            prev_nid = nid
        return links

    @staticmethod
    def make_path(net_graph, node_list=None, pathway=None):
        node_list = node_list or pathway.node_list
        return Path(links=Flow.find_links(net_graph, node_list), pathway=pathway)

    @staticmethod
    def make_paths(net_graph, node_lists=None, pathways=None):
        paths = []
        if node_lists is not None:
            for node_list in node_lists:
                paths.append(Flow.make_path(net_graph=net_graph, node_list=node_list))
        elif pathways is not None:
            for pathway in pathways:
                paths.append(Flow.make_path(net_graph=net_graph, pathway=pathway))
        else:
            print "@@@ exception raised"
            raise Exception("Make path without specifying either node_list or pathways")
        return paths

    def release(self):
        for path in self.paths:
            path.release()  # release all the bandwidth
        self.bandwidth_sum = self.cal_bandwidth_sum()
        assert self.bandwidth_sum == 0, "bandwidth_sum should be 0 after release, however it is: %d" % self.bandwidth_sum

    def simple_find_paths(self, begin=None, end=None):
        """
        This algorithm use DFS to find all the simple connection in between two nodes in a graph.
        For simple connection, we mean there could be overlap in between two nodes
        :return: connection paths
        """
        begin = begin or self.source_nid
        end = end or self.sink_nid
        visited_nids = [begin]
        paths = []

        def dfs_search():
            # closure to do the recursive find path
            neighbors = self.net_graph.link_dict.get_valid_neighbors(visited_nids[-1])

            for node_id in neighbors:
                if visited_nids.count(node_id) > 0:
                    continue
                if node_id == end:
                    visited_nids.append(node_id)
                    paths.append(visited_nids[:])
                    visited_nids.pop()
                    continue
                visited_nids.append(node_id)
                dfs_search()
                visited_nids.pop()

        dfs_search()
        return paths


class CoFlow(FlowBase):
    """
    Coflow is combination of flows which together determinate the job's start and end time
    http://conferences.sigcomm.org/hotnets/2012/papers/hotnets12-final51.pdf
    """
    def __init__(self, volume, id, start_end_volume_list=None, data=None, flow_refs=None):
        """
        :param volume: Total volume for the current flow
        :param start_end_volume_list: [((start_id, end_id), volume)] which defines flows
        """
        super(self.__class__, self).__init__(volume=volume, id=id)
        self._fid = 0
        self.flow_dict = {}
        self.flow_kv = None
        self.remain_completion_time = 0
        volume_acc = 0
        if flow_refs is None:
            start_end_volume_list = start_end_volume_list or self.make_start_end_volume_list(data)
            volume_acc = 0
            for id_pair, flow_volume in start_end_volume_list:
                volume_acc += flow_volume
                self.flow_dict[self._fid] = Flow(id_pair[0], id_pair[1], flow_volume, self.fid)
        else:
            for flow_ref in flow_refs:
                self.flow_dict[flow_ref.id] = flow_ref
                volume_acc += flow_ref.volume
        assert(round(volume_acc, 2) == round(volume, 2)), 'Inconsistent coflow input volume: %s and calculated flow volume:%s' %(
            volume, volume_acc)

    @property
    def fid(self):
        self._fid += 1
        return self._fid

    def print_flow_dict(self):
        flow_dict_info_str = "----- COFLOW'S FLOW_DICT -----\n"
        flow_dict_info_str += str(self.id) + " flow_dict: {\n"
        for k, v in self.flow_dict.items():
            flow_dict_info_str += "\t" + str(v) + "\n"
        flow_dict_info_str += "}"
        return flow_dict_info_str

    def make(self, net_graph, flow_kv=None):
        flow_kv = flow_kv or self.flow_kv
        if flow_kv is None:
            print "try to make coflow with net_graph solely"
            for flow in self.flow_dict.values():
                flow.make(net_graph=net_graph)
        else:
            for fid, flow in self.flow_dict.items():
                if fid not in flow_kv:
                    if round(flow.transmitted_volume, 2) != round(flow.total_volume, 2):
                        logger.info("flow_id: %s, total volume: %s, transmitted volume: %s" % (
                            str(fid), str(flow.total_volume), str(flow.transmitted_volume))
                                    + "path allocation for flow " + str(fid) + " has too small bandwidth")
                        print '\t' + str(fid) + ' ' + str(flow.volume)
                        print '\t0.0\tno path available'
                    else:
                        logger.info("flow_id: %s, total volume: %s, transmitted volume: %s" % (
                                    str(fid), str(flow.total_volume), str(flow.transmitted_volume))
                                    + "path allocation for flow " + str(fid) + " not needed, flow transition completed")
                        print '\t' + str(fid) + ' ' + str(flow.volume)
                        print '\t0.0\tflow already transmitted'
                    flow.paths = []
                else:
                    flow.make(net_graph=net_graph, link_kv=flow_kv[fid])

    def release(self):
        for _, flow in self.flow_dict.items():
            flow.release()

    def check_if_transmitted(self):
        for _, flow in self.flow_dict.items():
            if flow.volume != 0:
                return False
        return True

    @staticmethod
    def make_start_end_volume_list(data):
        start_end_volume_list = []
        end_node = data['DataDestination']
        source2net = CoFlow.read_source2net_from_net_node_data(data['NetNode'])
        for start_node, source_list in source2net.items():
            for source_id in source_list:
                start_end_volume_list.append(((start_node, end_node), data['DataSource'][source_id]))
        return start_end_volume_list

    @staticmethod
    def read_source2net_from_net_node_data(net_node_data):
        source2net = {}
        for net_node_id, value in net_node_data.items():
            if len(value['dataSources']) > 0:
                source2net[net_node_id] = value['dataSources']
        return source2net
