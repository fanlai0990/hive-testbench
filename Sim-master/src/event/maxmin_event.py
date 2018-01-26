from .event_base import Event
import logging

# setup logger
logger = logging.getLogger(__name__)


class MaxMinFlowEvent(Event):
    event_type = 'MaxMinFlowEvent'

    def __init__(self, timestamp, isStart, manager, scheduled_flow):
        super(MaxMinFlowEvent, self).__init__(timestamp=timestamp, isStart=isStart, manager=manager)
        self.flow = scheduled_flow
        self.remain_transmission_time = round(float(scheduled_flow.volume) / float(scheduled_flow.assigned_bw), 2)

    def start_event(self, event_queue, net_graph):
        self.timestamp += self.remain_transmission_time
        self.isStart = False
        event_queue.insert(self)

    def end_event(self, event_queue, net_graph):
        # logger.debug("print event_queue: " + str(event_queue))
        # when a flow transition is completed, find another flow to schedule
        self.manager.current_time = self.timestamp
        # release bandwidth
        self.flow.max_bw_path.release()
        self.flow.remain(remain_volume=0)
        self.flow.assigned_bw = 0
        # mark itself as complete in manager
        self.manager.complete_flow(flow=self.flow)
        # for job_events, we store them in a separate list and interrupt them at the end to prevent dead loop
        job_events = []
        # call interrupt on other events in the event queue to reschedule
        while not event_queue.empty():
            top_events = event_queue.pop()
            for top_event in top_events:
                if top_event.event_type == 'JobInsertionEvent':
                    job_events.append(top_event)
                else:
                    top_event.interrupt_event(event_queue, net_graph, self.timestamp)
        # insert job back to the event_queue
        for job_event in job_events:
            job_event.interrupt_event(event_queue, net_graph, self.timestamp)
        # add new schedule event
        event_queue.insert(MaxMinFlowScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        # logger.debug("in flow_end, after inserting maxminflow event: print event_queue: " + str(event_queue))

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        # update remaining volume
        remain_time = self.timestamp - interrupt_timestamp
        self.flow.remain(remain_time=remain_time, maxmin=True)
        # release bandwidth
        self.flow.max_bw_path.release()
        # do not need to put flow back to unscheduled flow dict
        self.flow.assigned_bw = 0

    def __str__(self):
        base_str = super(MaxMinFlowEvent, self).__str__()
        base_str += '  ' + self.flow.id
        return base_str


class MaxMinFlowScheduleEvent(Event):
    event_type = 'MaxMinScheduleEvent'

    def __init__(self, timestamp, isStart, manager):
        super(MaxMinFlowScheduleEvent, self).__init__(timestamp=timestamp, isStart=isStart, manager=manager)

    def start_event(self, event_queue, net_graph):
        self.manager.schedule()  # allocate_flow() is called here
        for flow_id, scheduled_flow in self.manager.scheduler.scheduled_flow_dict.items():
            # logger.debug("add flow_event: flow_id:" + str(flow_id))  # FIXME: no output, nothing in scheduled_flow_dict
            event_queue.insert(MaxMinFlowEvent(timestamp=self.timestamp,
                                               isStart=True,
                                               manager=self.manager,
                                               scheduled_flow=scheduled_flow))

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        # Hack around circular dependency caused by directly inherit from schedule event
        logger.info("complete at same time")
