from . import FlowScheduler
from src.exceptions import AllocationException, OptimizerException
import logging

# setup logger
logger = logging.getLogger(__name__)


class PoorManScheduler(FlowScheduler):
    """
    Schedule strategy:
    1. Recursively Schedule the coflow with minimum cct
    2. Schedule Remain coflows' flows separately
    """
    def schedule(self):
        if len(self.unscheduled_coflow_dict) == 0:
            return
        # pre-sort all the unscheduled coflows on the empty network
        self.sort_coflows()
        # allocate just as many coflows as we can
        for coflow_id, completion_time in self.sorted_coflow:
            coflow = self.unscheduled_coflow_dict[coflow_id]
            if self.remaining_bandwidth() == 0:
                return
            # print "\nTry to schedule a new coflow"
            logger.info("Try to schedule a unscheduled coflow")
            try:
                self.mmcf_coflow(coflow)
                self.allocate_coflow(coflow_id)
            except AllocationException as e:
                # print('Exception: ' + str(e))
                logger.debug('Exception: ' + str(e))
                self.unresolve_coflow(coflow_id)
        self.unscheduled_coflow_dict = self.coflows2coflow_dict(self.unresolved_coflows)
        self.unresolved_coflows = []

        # allocate as many flows as we can
        if len(self.unscheduled_coflow_dict) > 0:
            # print "\nTry to schedule separate flows"
            logger.info("Try to schedule separate flows")
            self.flow_schedule()

    def mmcf_coflow(self, coflow):
        try:
            completion_time, flow_kv = self.optimizer(coflow, self.net_graph)
            coflow.remain_completion_time = completion_time
            coflow.flow_kv = flow_kv
        except OptimizerException:
            raise AllocationException
