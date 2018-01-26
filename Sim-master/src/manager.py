from .event import EventQueue, ScheduleEvent, CombineScheduleEvent, MaxMinFlowScheduleEvent, JobInsertionEvent
from .network.net_graph import NetGraph
from .utils import make_coflow_dict
from .spark.mock_spark import MockSpark
from .scheduler.maxmin_flow_scheduler import MaxMinFlowScheduler

import csv
import os


class Manager(object):
    """
    Manager manages network and coflows
    """
    def __init__(self, gml_path, coflow_path):
        self.net_graph = NetGraph(gmlfilename=gml_path)
        self.complete_coflow_dict = dict()
        if coflow_path is None:
            self.coflow_dict = dict()
        else:
            self.coflow_dict = make_coflow_dict(fileName=coflow_path)


class EventManager(Manager):
    """
    An EventManager manages all the events and scheduling
    """
    def __init__(self, gml_path, coflow_path, scheduler_class):
        super(EventManager, self).__init__(gml_path=gml_path, coflow_path=coflow_path)
        self.current_time = 0
        self.event_queue = EventQueue()
        if scheduler_class != MaxMinFlowScheduler:
            self.scheduler = scheduler_class(net_graph=self.net_graph, coflow_dict=self.coflow_dict)
            self.is_flow_scheduler = self.scheduler.scheduler_type == 'FlowScheduler'
        else:
            self.scheduler = None
            self.is_flow_scheduler = True

    def complete_coflow(self, coflow):
        self.complete_coflow_dict[coflow.fid] = coflow
        self.scheduler.remove_coflow(coflow=coflow)

    def schedule(self):
        self.scheduler.schedule()

    def flow_schedule(self):
        assert self.scheduler.scheduler_type == 'FlowScheduler'
        self.scheduler.flow_schedule()


class SimulationManager(EventManager):
    """
    Simulation manager not only manage the resources, but also responsible for
    printing the simulation results, visualizing the results
    """
    def print_all_coflows(self):
        for coflow_id, coflow in self.coflow_dict.items():
            print "coflow id: ", coflow_id
            coflow.print_flow_dict()

    def visualize_netgraph(self):
        self.net_graph.visualize('test_visualize_file')

    def simulate(self):
        if self.is_flow_scheduler is False:
            self.event_queue.insert(ScheduleEvent(self.current_time, True, self))
        else:
            self.event_queue.insert(CombineScheduleEvent(self.current_time, True, self))

        while not self.event_queue.empty():
            print "\n-------------------------------------------------------------------"
            top_elements = self.event_queue.pop()
            for top_element in top_elements:
                # assert top_element.event_type == 'ScheduleEvent'
                top_element.process(self.event_queue, self.net_graph)

        print "\n==================================================================="
        print 'Simulator terminates @ time ' + str(round(self.current_time, 2)) + '\n'


class SparkEnableSimulationManager(SimulationManager):
    """
        Event manager supports for the spark simulation
    """
    def __init__(self, gml_path, spark_path, scheduler_class):
        super(SparkEnableSimulationManager, self).__init__(gml_path=gml_path, coflow_path=None, scheduler_class=scheduler_class)
        self.spark = MockSpark(self.net_graph, spark_path)
        self.coflow_timetable = dict()       # {coflow_id: {start_time: t, finish_time: t}}
        self.bandwidth_usage_dict = dict()   # {timestamp: allocated_bw_sum}
        self.total_bandwidth_on_network, _ = self.bandwidth_statistic()

    def simulate(self):
        total_bw_sum, allocated_bw_sum = self.bandwidth_statistic()
        assert allocated_bw_sum == round(float(0), 2)
        self.total_bandwidth_on_network = total_bw_sum

        for timestamp in self.spark.time_jobs_dict.keys():
            self.event_queue.insert(JobInsertionEvent(timestamp, True, self))
        while not self.event_queue.empty():
            # print "before processing: " + str(self.event_queue)
            print "\n-------------------------------------------------------------------"
            top_elements = self.event_queue.pop()
            for top_element in top_elements:
                top_element.process(self.event_queue, self.net_graph)
        print "\n==================================================================="
        print 'Simulator terminates @ time ' + str(round(self.current_time, 2))
        self.print_statistics()

    def print_statistics(self):
        sorted_coflow_completion_time = sorted(self.coflow_timetable.items(), key=lambda x: x[1]['start_time'])
        sorted_bandwidth_usage_dict = sorted(self.bandwidth_usage_dict.items(), key=lambda x: x[0])
        sorted_job_completion_time_dict = sorted(self.spark.job_completion_time_dict.items(), key=lambda x: x[1][0])

        with open(os.path.dirname(__file__) + '/../results/simulation/coflow/cct.csv', 'wb') as csvfile:
            statistics_writer = csv.writer(csvfile, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            statistics_writer.writerow(['CoflowID', 'StartTime', 'EndTime', 'CoflowCompletionTime'])
            for (coflow_id, time_dict) in sorted_coflow_completion_time:
                if 'end_time' not in time_dict:
                    print '\033[91m', 'DEBUG end_time not recorded for coflow', coflow_id, '\033[0m'
                statistics_writer.writerow([str(coflow_id), time_dict['start_time'], time_dict['end_time'], time_dict['end_time'] - time_dict['start_time']])
        print "Coflow Completion Time Statistic saved to results/simulation/coflow/cct.csv"

        with open(os.path.dirname(__file__) + '/../results/simulation/coflow/bw.csv', 'wb') as csvfile:
            statistics_writer = csv.writer(csvfile, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            statistics_writer.writerow(['Bandwidth', 'TimeStamp'])
            statistics_writer.writerow([0, 0])
            for (timestamp, allocated_bw) in sorted_bandwidth_usage_dict:
                statistics_writer.writerow([allocated_bw, timestamp])
        print "Bandwidth Usage Statistic saved to results/simulation/coflow/bw.csv"

        with open(os.path.dirname(__file__) + '/../results/simulation/coflow/job.csv', 'wb') as csvfile:
            statistics_writer = csv.writer(csvfile, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            statistics_writer.writerow(['JobID', 'StartTime', 'EndTime', 'JobCompletionTime'])
            for (job_id, time_tuple) in sorted_job_completion_time_dict:
                statistics_writer.writerow([job_id, time_tuple[0], time_tuple[1], time_tuple[1] - time_tuple[0]])
        print "Job Completion Time Statistic saved to results/simulation/coflow/job.csv"

    def add_jobs(self):
        coflow_dict = self.spark.get_running_coflows()
        self.update_coflow_dict(coflow_dict)

    def update_coflow_dict(self, coflow_dict):
        self.coflow_dict.update(coflow_dict)
        # pre-process to neglect the coflows whose flows are all zero-volume
        for coflow_id, coflow in coflow_dict.items():
            self.coflow_timetable[coflow_id] = {'start_time': self.current_time}
            if coflow.check_if_transmitted():
                # record finish time as the same time as now
                self.coflow_timetable[coflow_id]['end_time'] = self.current_time
                # record at complete_coflow_dict and remove from others
                self.complete_coflow_dict[coflow_id] = coflow
                # get new coflows to run
                coflow_dict = self.spark.complete_coflow(coflow_id=coflow.id, update_time_stamp=self.current_time)
                self.update_coflow_dict(coflow_dict)
            else:
                self.scheduler.unscheduled_coflow_dict.update(coflow_dict)

    def complete_coflow(self, coflow):
        # record finish time
        assert coflow.id in self.coflow_timetable, 'coflow id %s' % str(coflow.id)
        self.coflow_timetable[coflow.id]['end_time'] = self.current_time
        # record at complete_coflow_dict and remove from others
        self.complete_coflow_dict[coflow.id] = coflow
        self.scheduler.remove_coflow(coflow=coflow)
        # get new coflows to run
        coflow_dict = self.spark.complete_coflow(coflow_id=coflow.id, update_time_stamp=self.current_time)
        self.update_coflow_dict(coflow_dict)

    def bandwidth_statistic(self):
        total_bandwidth_sum = 0
        allocated_bandwidth_sum = 0
        all_links_on_network = self.net_graph.link_dict.get_dict()
        for link_id, link in all_links_on_network.items():
            total_bandwidth_sum += link.total_bandwidth
            allocated_bandwidth_sum += link.allocated_bandwidth
        return total_bandwidth_sum, allocated_bandwidth_sum


class BaselineSparkSimulationManager(SparkEnableSimulationManager):
    FLOW_INDICATOR = ':FLOW:'

    def __init__(self, gml_path, spark_path):
        super(BaselineSparkSimulationManager, self).__init__(gml_path, spark_path, MaxMinFlowScheduler)
        self.flow_dict = dict()
        self.running_coflow_flow_map = dict()
        self.flow_coflow_map = dict()
        self.scheduler = MaxMinFlowScheduler(self.net_graph, self.flow_dict)

    def update_flow_dict(self, flow_dict):
        self.flow_dict.update(flow_dict)
        self.scheduler.unscheduled_flow_dict.update(flow_dict)

    def update_coflow_dict(self, coflow_dict):
        self.coflow_dict.update(coflow_dict)
        flow_dict, coflow_flow_map, flow_coflow_map = self.unwind_coflow_dict(coflow_dict)
        self.update_flow_dict(flow_dict)
        self.running_coflow_flow_map.update(coflow_flow_map)
        self.flow_coflow_map.update(flow_coflow_map)

    def complete_flow(self, flow):
        coflow_id = self.flow_coflow_map[flow.id]
        self.running_coflow_flow_map[coflow_id].remove(flow.id)
        # remove the flow from the scheduled_flow_dict
        self.scheduler.remove_flow(flow=flow, complete_remove=True)
        # if there is no running flow for running coflow, mark it as complete
        if len(self.running_coflow_flow_map[coflow_id]) == 0:
            coflow = self.coflow_dict[coflow_id]
            self.complete_coflow(coflow)
            del self.running_coflow_flow_map[coflow_id]

    def complete_coflow(self, coflow):
        self.complete_coflow_dict[coflow.id] = coflow
        coflow_dict = self.spark.complete_coflow(coflow_id=coflow.id, update_time_stamp=self.current_time)
        self.update_coflow_dict(coflow_dict)

    def print_statistics(self):
        sorted_bandwidth_usage_dict = sorted(self.bandwidth_usage_dict.items(), key=lambda x: x[0])
        sorted_job_completion_time_dict = sorted(self.spark.job_completion_time_dict.items(), key=lambda x: x[1][0])

        with open(os.path.dirname(__file__) + '/../results/simulation/baseline/bw.csv', 'wb') as csvfile:
            statistics_writer = csv.writer(csvfile, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            statistics_writer.writerow(['Bandwidth', 'TimeStamp'])
            statistics_writer.writerow([0, 0])
            for (timestamp, allocated_bw) in sorted_bandwidth_usage_dict:
                statistics_writer.writerow([allocated_bw, timestamp])
        print "Bandwidth Usage Statistic saved to results/simulation/baseline/bw.csv"

        with open(os.path.dirname(__file__) + '/../results/simulation/baseline/job.csv', 'wb') as csvfile:
            statistics_writer = csv.writer(csvfile, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            statistics_writer.writerow(['JobID', 'StartTime', 'EndTime', 'JobCompletionTime'])
            for (job_id, time_tuple) in sorted_job_completion_time_dict:
                statistics_writer.writerow([job_id, time_tuple[0], time_tuple[1], time_tuple[1] - time_tuple[0]])
        print "Job Completion Time Statistic saved to results/simulation/baseline/job.csv"

    @staticmethod
    def unwind_coflow_dict(coflow_dict):
        flow_dict = {}
        coflow_flow_map = {}
        flow_coflow_map = {}
        for coflow_id, coflow in coflow_dict.items():
            coflow_flow_map[coflow_id] = set()
            for flow_id, flow in coflow.flow_dict.items():
                unique_id = coflow_id + BaselineSparkSimulationManager.FLOW_INDICATOR + str(flow.id)
                flow.id = unique_id # overwrite flow.id for global uniqueness
                flow_dict[unique_id] = flow
                coflow_flow_map[coflow_id].add(unique_id)
                flow_coflow_map[unique_id] = coflow_id
        return flow_dict, coflow_flow_map, flow_coflow_map
