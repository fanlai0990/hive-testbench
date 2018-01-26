from src.exceptions import SchedulerException
from .event_base import Event
from .maxmin_event import MaxMinFlowScheduleEvent
import logging


# setup logger
logger = logging.getLogger(__name__)


class CoflowEvent(Event):
    event_type = 'CoflowEvent'
    """
    A CoflowEvent is created by a coflow scheduler, it corresponds to a scheduled coflow.
    """
    def __init__(self, timestamp, isStart, manager, scheduled_coflow):
        self.coflow = scheduled_coflow
        self.transmission_time = self.coflow.remain_completion_time
        super(CoflowEvent, self).__init__(timestamp=timestamp, isStart=isStart, manager=manager)

    def start_event(self, event_queue, net_graph):
        self.timestamp += self.transmission_time
        self.isStart = False
        event_queue.insert(self)  # transform to end_event

    def end_event(self, event_queue, net_graph):
        # Reschedule the coflow
        self.manager.current_time = self.timestamp
        # Complete the coflow
        self.coflow.release()
        self.manager.complete_coflow(self.coflow)
        logger.debug('event_queue: ' + str(event_queue))
        # Interrupt the rest of the coflows execution
        # for job_events, we store them in a separate list and interrupt them at the end to prevent dead loop
        job_events = []
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
        # event_queue.insert(ScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        if self.manager.scheduler.scheduler_type == "Scheduler":
            event_queue.insert(ScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        elif self.manager.scheduler.scheduler_type == "FlowScheduler":
            event_queue.insert(CombineScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        else:
            raise SchedulerException("No such type of scheduler")

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        assert interrupt_timestamp < self.timestamp, 'interrupt_time: %d > timestamp: %d' % (interrupt_timestamp,
                                                                                             self.timestamp)
        remain_time = self.timestamp - interrupt_timestamp
        for fid, flow in self.coflow.flow_dict.items():
            flow.remain(remain_time=remain_time)
        self.coflow.release()

        if self.coflow.check_if_transmitted():
            # self.isStart = False
            self.print_event()
            self.manager.complete_coflow(self.coflow)
        else:
            self.manager.scheduler.reschedule_coflow(self.coflow)
        # self.coflow.print_flow_dict()

    def __str__(self):
        coflow_start_time = self.manager.coflow_timetable[self.coflow.id]['start_time']
        base_str = super(CoflowEvent, self).__str__()
        base_str += 'CoflowID ' + self.coflow.id + \
                    '\tStartTime %.4f ' % coflow_start_time
        if self.isStart is False:
            base_str += 'CoflowCompletionTime %.4f' % (self.timestamp - coflow_start_time)
        return base_str


class FlowEvent(Event):
    event_type = 'FlowEvent'
    """
    A FlowEvent is either created by a CoflowEvent or created by a flow level scheduler.
    """
    def __init__(self, timestamp, isStart, manager, belonged_coflow_id, schedule_flow_id, schedule_flow):
        super(self.__class__, self).__init__(timestamp=timestamp, isStart=isStart, manager=manager)
        self.belonged_coflow_id = belonged_coflow_id
        self.schedule_flow_id = schedule_flow_id
        self.flow = schedule_flow

    def start_event(self, event_queue, net_graph):
        assert self.flow.bandwidth_sum != 0, "flow scheduled on zero bandwidth"
        transmission_time = self.flow.volume / float(self.flow.bandwidth_sum)
        self.timestamp += transmission_time
        self.isStart = False
        event_queue.insert(self)

    def end_event(self, event_queue, net_graph):
        # set manager time
        self.manager.current_time = self.timestamp
        # release bandwidth
        self.flow.remain(remain_volume=0)
        self.flow.release()
        # remove itself from the scheduled_flow_dict
        self.manager.scheduler.remove_flow(self.belonged_coflow_id, self.schedule_flow_id)
        # check if coflow finishes
        belonged_coflow = self.manager.scheduler.unscheduled_coflow_dict[self.belonged_coflow_id]
        if belonged_coflow.check_if_transmitted():
            self.manager.complete_coflow(belonged_coflow)
        # when a flow transition is completed, find another flow to schedule
        event_queue.insert(FlowScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        assert interrupt_timestamp < self.timestamp, \
            'error because interrupt_time: %d > timestamp: %d' % (interrupt_timestamp, self.timestamp)
        # update remaining volume
        remain_time = self.timestamp - interrupt_timestamp
        self.flow.remain(remain_time=remain_time)
        self.flow.release()
        # remove itself from the scheduled_flow_dict
        self.manager.scheduler.remove_flow(self.belonged_coflow_id, self.schedule_flow_id)
        # need not reschedule on flow event

    def __str__(self):
        base_str = super(FlowEvent, self).__str__()
        base_str += 'CoflowID ' + self.belonged_coflow_id + \
                    '\tFlowID ' + str(self.schedule_flow_id)
        return base_str


class ScheduleEvent(Event):
    event_type = 'ScheduleEvent'
    """
    Generate flow event for the scheduled flows
    """
    def __init__(self, timestamp, isStart, manager):
        super(ScheduleEvent, self).__init__(timestamp=timestamp, isStart=isStart, manager=manager)

    def start_event(self, event_queue, net_graph):
        self.manager.schedule()
        for coflow_id, scheduled_coflow in self.manager.scheduler.scheduled_coflow_dict.items():
            event_queue.insert(CoflowEvent(timestamp=self.timestamp,
                                           isStart=True,
                                           manager=self.manager,
                                           scheduled_coflow=scheduled_coflow))

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        logger.info("complete at same time")


class CombineScheduleEvent(ScheduleEvent):
    event_type = 'CombineScheduleEvent'
    """
    Flow version of the ScheduleEvent
    Not only puts CoflowEvent in the queue, but also FlowEvent
    """
    def start_event(self, event_queue, net_graph):
        # put coflows-to-be-scheduled in scheduled_coflow_dict
        # put flows-to-be-scheduled in inserting_flow_dict
        logger.debug("combine schedule, event_queue: " + str(event_queue))
        self.manager.schedule()
        for coflow_id, scheduled_coflow in self.manager.scheduler.scheduled_coflow_dict.items():
            event_queue.insert(CoflowEvent(timestamp=self.timestamp,
                                           isStart=True,
                                           manager=self.manager,
                                           scheduled_coflow=scheduled_coflow))
        for coflow_id, schedule_flows in self.manager.scheduler.inserting_flow_dict.items():
            for flow_id, flow_ref in schedule_flows.items():
                event_queue.insert(FlowEvent(timestamp=self.timestamp,
                                             isStart=True,
                                             manager=self.manager,
                                             belonged_coflow_id=coflow_id,
                                             schedule_flow_id=flow_id,
                                             schedule_flow=flow_ref))
        self.manager.scheduler.inserting_flows_to_scheduled()


class FlowScheduleEvent(ScheduleEvent):
    event_type = 'FlowScheduleEvent'
    '''
    called when a separate flow completes, find another flow/flows to utilize the bandwidth
    '''
    def start_event(self, event_queue, net_graph):
        self.manager.flow_schedule()  # putting flows-to-be-scheduled in inserting_flow_dict
        for coflow_id, schedule_flows in self.manager.scheduler.inserting_flow_dict.items():
            for flow_id, flow_ref in schedule_flows.items():
                event_queue.insert(FlowEvent(timestamp=self.timestamp,
                                             isStart=True,
                                             manager=self.manager,
                                             belonged_coflow_id=coflow_id,
                                             schedule_flow_id=flow_id,
                                             schedule_flow=flow_ref))
        self.manager.scheduler.inserting_flows_to_scheduled()


class JobInsertionEvent(ScheduleEvent):
    event_type = 'JobInsertionEvent'

    def __init__(self, timestamp, isStart, manager):
        super(JobInsertionEvent, self).__init__(timestamp, isStart, manager)

    def start_event(self, event_queue, net_graph):
        total_bw_sum, allocated_bw_sum = self.manager.bandwidth_statistic()
        if allocated_bw_sum != 0:
            print 'BandwidthUsage %.4f/%.4f' % (allocated_bw_sum, total_bw_sum)
            self.manager.bandwidth_usage_dict[self.timestamp] = allocated_bw_sum

        self.manager.spark.job_arrives(timestamp=self.timestamp)
        self.manager.add_jobs()
        job_events = []
        while not event_queue.empty():
            top_events = event_queue.pop()
            for top_event in top_events:
                if top_event.event_type == 'JobInsertionEvent':
                    job_events.append(top_event)
                else:
                    top_event.interrupt_event(event_queue, net_graph, self.timestamp)
        for job_event in job_events:
            job_event.interrupt_event(event_queue, net_graph, self.timestamp)
        # Reschedule the coflow
        self.manager.current_time = self.timestamp
        if self.manager.scheduler.scheduler_type == "Scheduler":
            event_queue.insert(ScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        elif self.manager.scheduler.scheduler_type == "FlowScheduler":
            event_queue.insert(CombineScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        elif self.manager.scheduler.scheduler_type == "MaxMinFlowScheduler":
            event_queue.insert(MaxMinFlowScheduleEvent(timestamp=self.timestamp, isStart=True, manager=self.manager))
        else:
            raise SchedulerException("No such type of scheduler")

    def interrupt_event(self, event_queue, net_graph, interrupt_timestamp):
        assert interrupt_timestamp < self.timestamp, \
            'error because interrupt_time: %d > timestamp: %d' % (interrupt_timestamp, self.timestamp)
        event_queue.insert(self)  # job haven't inserted yet, put it back to event queue
