import copy
from src.mmcf import mmcf_optimize
from src.network.flow import Flow, CoFlow
import logging

# setup logger
logger = logging.getLogger(__name__)

'''
- Run MCF for all flows (OR Run "All-Pair-Shortest-Paths" to map paths to flows)

- Count the number of flows per link:
    foreach f in F:
        for each l in path[f]:
            flows_on_link[l].add(f)

- Calculate bandwidth
    F_ = F
    foreach link l in N:
        rem[l] = CAP[l]
    foreach f in F_:
        bw[f] = 0

    while (len(F_) > 0)
        foreach f in F_:
            bw_[f] = INF

        foreach f in F_:
            foreach l in path[f]:
                bw_[f] = min(bw_[f], rem[l] / len(flows_on_link[l]))

        foreach link l in N:
            foreach f in flows_on_link[l]:
                rem[l] -= bw_[f]

        foreach f in F_:
            if bw_[f] ~ 0:
                F_.remove(f)
                foreach l in path[f]:
                    flows_on_link[l].remove(f)
            else
                bw[f] += bw_[f]
'''


class MaxMinFlowScheduler(object):
    scheduler_type = 'MaxMinFlowScheduler'

    def __init__(self, net_graph, flow_dict, optimizer=None):
        self.flow_dict = flow_dict
        self.unscheduled_flow_dict = flow_dict.copy()
        self.scheduled_flow_dict = dict()
        self.net_graph = net_graph
        self.optimizer = optimizer or mmcf_optimize

    def remove_flow(self, flow, complete_remove=False):
        if complete_remove:
            if flow.id in self.flow_dict:
                del self.flow_dict[flow.id]  # this statement also removes flow from self.scheduled_flow_dict
            else:
                print "DEBUG flow id: %s unscheduled flow dict size: %d scheduled flow dict size: %d" \
                      % (flow.id, len(self.unscheduled_flow_dict), len(self.scheduled_flow_dict))
                raise Exception('Remove a not existing flow')

            if flow.max_bw_path is not None:
                for link in flow.max_bw_path.links:
                    if flow.id in link.flow_dict:
                        del link.flow_dict[flow.id]

        if flow.id in self.unscheduled_flow_dict:
            del self.unscheduled_flow_dict[flow.id]
        elif flow.id in self.scheduled_flow_dict:
            del self.scheduled_flow_dict[flow.id]
        else:
            print "DEBUG flow id: %s unscheduled flow dict size: %d scheduled flow dict size: %d" \
                  % (flow.id, len(self.unscheduled_flow_dict), len(self.scheduled_flow_dict))
            raise Exception('Remove a not existing flow')

    def reassign_flow(self, flow):
        self.remove_flow(flow)
        self.unscheduled_flow_dict[flow.id] = flow

    def schedule(self):
        # find path for new flows
        if len(self.unscheduled_flow_dict) > 0:
            # run mmcf for all flows together
            flow_ref_list = list()
            coflow_volume = 0
            for _, flow in self.unscheduled_flow_dict.items():
                # use copy instead of assignments and then set all flows to have the same volume
                flow1 = copy.copy(flow)
                flow1.volume = 1000
                coflow_volume += flow1.volume
                flow_ref_list.append(flow1)
            mock_coflow = CoFlow(id=0, volume=coflow_volume, flow_refs=flow_ref_list)
            completion_time, flow_kv = self.optimizer(mock_coflow, self.net_graph)

            # find path according to mmcf results
            for _, flow in self.unscheduled_flow_dict.items():
                paths_list = flow.find_pathway_with_link_allocation(flow_kv[flow.id])
                max_bw_pathway = max(paths_list, key=lambda x: x.bandwidth)
                flow.max_bw_path = Flow.make_path(net_graph=self.net_graph, pathway=max_bw_pathway)

        # flows we need to reassign bw
        flow_dict_check = self.unscheduled_flow_dict.copy()
        flow_dict_check.update(self.scheduled_flow_dict)

        # Count the number of flows per link:
        for _, flow in flow_dict_check.items():
            for link in flow.max_bw_path.links:
                link.flow_dict[flow.id] = flow

        # calculate bandwidth
        while len(flow_dict_check) > 0:
            flow_bw_dict = dict()
            for _, flow in flow_dict_check.items():
                flow_bw_dict[flow.id] = float('inf')
                for link in flow.max_bw_path.links:
                    # if the sum of average bw exceeds the limit, then decrement each link bw assignment by 0.01
                    # may lose some bandwidth, but not by a significant amount
                    average_bw_on_link = round(float(link.bandwidth) / float(len(link.flow_dict)), 2)
                    if round(average_bw_on_link * len(link.flow_dict), 2) > round(float(link.bandwidth), 2):
                        average_bw_on_link -= 0.01

                    flow_bw_dict[flow.id] = round(min(flow_bw_dict[flow.id], average_bw_on_link), 2)

            net_graph_link_kv = self.net_graph.link_dict.get_dict()
            for _, link in net_graph_link_kv.items():
                for flow_id, flow in link.flow_dict.items():
                    link.allocate(flow_bw_dict[flow_id])

            for flow_id, flow in flow_dict_check.items():
                if flow_bw_dict[flow_id] < 0.1:
                    del flow_dict_check[flow_id]
                    for link in flow.max_bw_path.links:
                        del link.flow_dict[flow_id]
                else:
                    flow.assigned_bw += flow_bw_dict[flow.id]

        print 'NEW flow allocation'
        # logger.info('Allocation for newly arrived flows')
        for flow_id, flow in self.unscheduled_flow_dict.items():
            # logger.info('Flow id ' + str(flow_id) + ' assigned bandwidth ' + str(flow.assigned_bw) + ' on links:')
            flow.max_bw_path.print_path()
            node_list_str = str()
            for node in flow.max_bw_path.pathway.node_list:
                node_list_str += str(node) + ' '
            print '\t' + str(flow_id) + '\t' + str(flow.assigned_bw) + '\t' + node_list_str
        print 'OLD flow allocation'
        for flow_id, flow in self.scheduled_flow_dict.items():
            # logger.info('Flow id ' + str(flow_id) + ' assigned bandwidth ' + str(flow.assigned_bw) + ' on links:')
            flow.max_bw_path.print_path()
            node_list_str = str()
            for node in flow.max_bw_path.pathway.node_list:
                node_list_str += str(node) + ' '
            print '\t' + str(flow_id) + '\t' + str(flow.assigned_bw) + '\t' + node_list_str

        self.scheduled_flow_dict.update(self.unscheduled_flow_dict)
        self.unscheduled_flow_dict = dict()
