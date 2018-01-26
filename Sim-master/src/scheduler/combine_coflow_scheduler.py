from src.network.flow import CoFlow
from . import Scheduler
from src.exceptions import AllocationException


class CombineCoflowScheduler(Scheduler):
    def __init__(self, net_graph, coflow_dict):
        super(self.__class__, self).__init__(net_graph=net_graph, coflow_dict=coflow_dict)
        self.combined_coflow = None
        # FIXME: combined_coflow_id causes eventual crash, because coflow_id doesn't follow the specified string format
        self.combined_coflow_id = 0
        self.combined_coflow_volume = 0
        self.backup_unscheduled_coflow_dict = dict()

    def schedule(self):
        if len(self.unscheduled_coflow_dict) > 0:
            self.backup_unscheduled_coflow_dict = self.unscheduled_coflow_dict
            flow_refs_list = []
            for coflow_id, coflow in self.unscheduled_coflow_dict.items():
                for _, flow in coflow.flow_dict.items():
                    flow_refs_list.append(flow)
                    self.combined_coflow_volume += flow.volume
            self.combined_coflow = CoFlow(id=self.combined_coflow_id,
                                          volume=self.combined_coflow_volume,
                                          flow_refs=flow_refs_list)
            self.unscheduled_coflow_dict.clear()
            self.unscheduled_coflow_dict[self.combined_coflow_id] = self.combined_coflow
            try:
                self.sort_coflows()
                self.allocate_minimum()
            except AllocationException as e:
                print str(e)
                self.unscheduled_coflow_dict = self.backup_unscheduled_coflow_dict
