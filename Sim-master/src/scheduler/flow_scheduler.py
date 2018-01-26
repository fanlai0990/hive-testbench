from collections import defaultdict
from . import Scheduler
from src.network.flow import Flow, CoFlow
from src.exceptions import AllocationException, OptimizerException
import logging

# setup logger
logger = logging.getLogger(__name__)


class FlowScheduler(Scheduler):
    scheduler_type = 'FlowScheduler'

    def __init__(self, net_graph, coflow_dict):
        super(FlowScheduler, self).__init__(net_graph=net_graph, coflow_dict=coflow_dict)
        self.scheduled_flow_dict = defaultdict(dict)  # { coflow_id :{ flow_id : flow_ref} }
        self.inserting_flow_dict = defaultdict(dict)  # { coflow_id :{ flow_id : flow_ref} }
        self.sorted_unscheduled_flow_list = list()    # [(coflow_id, flow_ref)]

    def sort_unscheduled_flows(self):
        self.sorted_unscheduled_flow_list = list()
        for coflow_id, coflow in self.unscheduled_coflow_dict.items():
            for flow_id, flow in coflow.flow_dict.items():
                if coflow_id in self.scheduled_flow_dict and flow_id in self.scheduled_flow_dict[coflow_id]:
                    logger.info("coflow: " + str(coflow_id) + "flow: " + str(flow.id) + " already scheduled")
                    # print "flow: " + str(flow_id) + " already scheduled"
                elif flow.volume == round(0.0, 2):
                    logger.info("coflow: " + str(coflow_id) + "flow: " + str(flow.id) + " already completed")
                    # print "flow: " + str(flow_id) + " already completed"
                else:
                    self.sorted_unscheduled_flow_list.append((coflow_id, flow))
        self.sorted_unscheduled_flow_list = sorted(self.sorted_unscheduled_flow_list, key=lambda x: x[1].volume)

    def allocate_flow(self, coflow_id, flow_id, optimize=False):
        if not optimize:
            try:
                self.unscheduled_coflow_dict[coflow_id].flow_dict[flow_id].make(net_graph=self.net_graph)
            except Exception as e:
                raise AllocationException("Allocation error due to: " + str(e))
        else:
            try:
                flow_ref = self.unscheduled_coflow_dict[coflow_id].flow_dict[flow_id]
                flow_list = list()
                flow_list.append(flow_ref)  # coflow that contains only one flow
                flow_coflow = CoFlow(id='-1', volume=flow_ref.volume, flow_refs=flow_list)
                completion_time, flow_kv = self.optimizer(flow_coflow, self.net_graph)
                if len(flow_kv[1]) == 0:
                    raise OptimizerException('given coflow cannot be allocated on current network')
                flow_ref.make(net_graph=self.net_graph, link_kv=flow_kv[1], coflow_id=coflow_id)
            except OptimizerException as e:
                # print "OptimizerException: " + str(e) + ", current coflow id: " + str(coflow_id)
                raise AllocationException("Allocation error due to optimizer failure: " + str(e))

    def remove_flow(self, belonged_coflow_id, schedule_flow_id):
        if belonged_coflow_id not in self.scheduled_flow_dict:
            raise Exception('Removing a flow whose coflow is not scheduled')
        elif schedule_flow_id not in self.scheduled_flow_dict[belonged_coflow_id]:
            raise Exception('Removing a unscheduled flow in a coflow')
        else:
            del self.scheduled_flow_dict[belonged_coflow_id][schedule_flow_id]
            if len(self.scheduled_flow_dict[belonged_coflow_id]) == 0:
                del self.scheduled_flow_dict[belonged_coflow_id]

    def flow_schedule(self):
        if self.remaining_bandwidth() > 0:
            self.sort_unscheduled_flows()
            for (coflow_id, flow) in self.sorted_unscheduled_flow_list:
                flow_ref = self.unscheduled_coflow_dict[coflow_id].flow_dict[flow.id]
                src = flow_ref.source_nid
                dst = flow_ref.sink_nid
                sp = self.net_graph.APSP[src][dst]
                min_bw = 1000000000
                for x in range(len(sp) - 1):
                    bw = self.net_graph.link_dict.get(start_node_id=sp[x], end_node_id=sp[x+1]).bandwidth
                    if bw < min_bw:
                        min_bw = bw

                if min_bw > 0:
                    path = flow.make_path(self.net_graph, sp)
                    path.allocate(min_bw)
                    flow.paths = [path]
                    flow.bandwidth_sum = min_bw
                    self.inserting_flow_dict[coflow_id][flow.id] = flow

    def flow_schedule_old(self):
        # allocate as much coflows as a whole as we can
        if self.remaining_bandwidth() > 0:
            # sort flows in from all coflows by their volume
            self.sort_unscheduled_flows()
            # try to allocate each flow
            for (coflow_id, flow) in self.sorted_unscheduled_flow_list:
                try:
                    if_allocated = True
                    self.allocate_flow(coflow_id, flow.id, optimize=True)  # optimize flow allocation using mmcf
                except AllocationException as e:
                    if_allocated = False
                    # print e
                    # print "Fail to allocate separate flow: " + str(flow_id) + ", not enough bandwidth"
                    logger.debug(str(e))
                    logger.debug("Fail to allocate separate flow: " + str(flow.id) + "in coflow: " + str(coflow_id))

                if if_allocated is True:
                    self.inserting_flow_dict[coflow_id][flow.id] = flow
                    # print "flow from coflow: " + str(coflow_id) + " with flow id: " + str(flow_id) + " scheduled"
                    logger.info("flow: " + str(flow.id) + " of coflow: " + str(coflow_id) + " scheduled")
                if self.remaining_bandwidth() == 0:
                    break

    def inserting_flows_to_scheduled(self):
        for coflow_id, inserting_flows in self.inserting_flow_dict.items():
            for flow_id, flow_ref in inserting_flows.items():
                self.scheduled_flow_dict[coflow_id][flow_id] = flow_ref
        self.inserting_flow_dict = defaultdict(dict)
