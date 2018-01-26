class Event(object):
    event_type = 'Event'
    """
    Each event class is an event + event handler which handles the event it represents.
    """
    def __init__(self, timestamp, isStart, manager, lag=0):
        self.start_time = timestamp
        self.timestamp = timestamp
        self.isStart = isStart
        self.manager = manager
        self.lag = lag

    def start_event(self, event_queue, net_graph):
        raise Exception(self.event_type, 'Unimplemented')

    def end_event(self, event_queue, net_graph):
        raise Exception(self.event_type, 'Unimplemented')

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        raise Exception('Unimplemented: %s' % self.event_type)

    def process(self, event_queue, net_graph):
        self.print_event()
        if self.isStart:
            self.start_event(event_queue, net_graph)
        else:
            total_bw_sum, allocated_bw_sum = self.manager.bandwidth_statistic()
            if allocated_bw_sum != 0:
                print 'BandwidthUsage %.4f/%.4f' % (allocated_bw_sum, total_bw_sum)
                self.manager.bandwidth_usage_dict[self.timestamp] = allocated_bw_sum
            self.end_event(event_queue, net_graph)

    def __str__(self):
        self_str = "@time %.4f" % self.timestamp + " " + str(self.event_type) + " "
        if self.isStart:
            self_str += "Start "
        else:
            self_str += "End   "
        return self_str

    def print_event(self):
        print self.__str__()
