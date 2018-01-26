from . import FlowScheduler
from src.exceptions import AllocationException
import logging

# setup logger
logger = logging.getLogger(__name__)


class RecursiveRemainScheduler(FlowScheduler):
    """
    Schedule strategy:
    1. Recursively Schedule the coflow with minimum cct
    2. Schedule Remain coflows' flows separately
    """
    def schedule(self):
        # allocate just as many coflows as we can
        while self.remaining_bandwidth() > 0 and len(self.unscheduled_coflow_dict) > 0:
            # print "\nTry to schedule a new coflow"
            logger.info("Try to schedule a unscheduled coflow")
            try:
                self.sort_coflows()
                if len(self.sorted_coflow) != 0:
                    self.allocate_minimum()  # accept the exception caused by this call
            except AllocationException as e:
                print('Exception: ' + str(e))
                logger.debug('Exception: ' + str(e))
                self.unresolve_coflow(self.minimum_id)
        self.unscheduled_coflow_dict = self.coflows2coflow_dict(self.unresolved_coflows)
        self.unresolved_coflows = []

        # allocate as many flows as we can
        if len(self.unscheduled_coflow_dict) > 0:
            # print "\nTry to schedule separate flows"
            logger.info("Try to schedule separate flows")
            self.flow_schedule_old()
