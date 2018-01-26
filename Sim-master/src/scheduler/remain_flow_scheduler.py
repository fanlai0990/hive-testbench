from . import Scheduler, FlowScheduler
from src.exceptions import AllocationException


class RemainFlowScheduler(FlowScheduler):
    """
    Schedule strategy:
    1. Schedule the coflow with minimum cct
    2. Schedule other coflows' flows separately
    """
    def schedule(self):
        # allocate just one coflow first
        print "Remaining bandwidth: "
        if self.remaining_bandwidth() > 0 and len(self.unscheduled_coflow_dict) > 0:
            print "\nTry to schedule a new coflow"
            # self.remaining_bandwidth()
            try:
                self.sort_coflows()
                self.allocate_minimum()
            except AllocationException as e:
                print('exception: ' + str(e))
                self.unresolve_coflow(self.minimum_id)
                self.unscheduled_coflow_dict = self.coflows2coflow_dict(self.unresolved_coflows)
                self.unresolved_coflows = []

        # allocate as much coflows as a whole as we can
        if len(self.sorted_coflow) > 0:
            print "\nTry to schedule separate flows"
            self.flow_schedule()
            self.inserting_flows_to_scheduled()
