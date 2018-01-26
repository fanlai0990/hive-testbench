from . import Scheduler
from src.exceptions import OptimizerException,AllocationException


class RecursiveCoflowScheduler(Scheduler):
    """
    Schedule strategy:
    1. Recursively Schedule the coflow with minimum cct
    """
    def schedule(self):
        print "Remaining bandwidth: "
        while self.remaining_bandwidth() > 0 and len(self.unscheduled_coflow_dict) > 0:
            print "\nTry to schedule a new coflow"
            # self.remaining_bandwidth()
            try:
                self.sort_coflows()
                self.allocate_minimum()
            except AllocationException as e:
                print str(e)
                self.unresolve_coflow(self.minimum_id)
        self.unscheduled_coflow_dict = self.coflows2coflow_dict(self.unresolved_coflows)
        self.unresolved_coflows = []
