from . import Scheduler


class ReverseRecursiveCoflowScheduler(Scheduler):

    def schedule(self):
        while self.remaining_bandwidth() > 0 and len(self.unscheduled_coflow_dict) > 0:
            print "remaining bandwidth: ", self.remaining_bandwidth()
            try:
                self.sort_coflows()
                self.allocate_maximum()
            except Exception as e:
                print('exception: ' + str(e))
                self.unresolve_coflow(self.minimum_fid)
        self.unscheduled_coflow_dict = self.coflows2coflow_dict(self.unresolved_coflows)
        self.unresolved_coflows = []

    def remaining_bandwidth(self):
        bandwidth_sum = 0
        for links, bandwidth in self.net_graph.link_dict.get_link_kv().items():
            print "links: ", links, " bandwidth: ", bandwidth
            bandwidth_sum += bandwidth
        return bandwidth_sum
