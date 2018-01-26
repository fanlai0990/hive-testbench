# A spark simulator class
from . import TraceReader
from .stage import getJobID
import logging

# setup logger
logger = logging.getLogger(__name__)


class MockSpark(object):
    def __init__(self, net_graph, job_file, job_reader_class=TraceReader, verbose=True):
        self.net_graph = net_graph
        reader = job_reader_class(job_file, net_graph)
        self.job_dict = reader.create_job_dict()
        self.time_jobs_dict = {}  # {time: jobs}
        self.arrived_job_dict = {}  # {job_id: job}
        self.group_jobs_by_time()
        if verbose:
            # Print the stages if it is verbose
            for job_id, job in self.job_dict.items():
                # print "job_id: ", job_id
                logger.info("job_id: " + str(job_id))
                job.print_stages()
        self.job_completion_time_dict = dict()  # {job_id: (start_time, end_time)}

    def get_running_coflows(self):
        coflow_dict = {}
        for job in self.arrived_job_dict.values():
            # assert len(job.running_stage_dict) == 0
            coflow_dict.update(job.run(self))
        return coflow_dict

    def job_arrives(self, timestamp=0.0):
        arrived_jobs = self.time_jobs_dict[timestamp]
        for arrived_job in arrived_jobs:
            assert arrived_job.job_id not in self.arrived_job_dict
            self.arrived_job_dict[arrived_job.job_id] = arrived_job

    def group_jobs_by_time(self):
        for job in self.job_dict.values():
            if job.start_time in self.time_jobs_dict:
                self.time_jobs_dict[job.start_time].append(job)
            else:
                self.time_jobs_dict[job.start_time] = [job]

    def complete_coflow(self, coflow_id, update_time_stamp):
        # print "complete_coflow: ", coflow_id
        job_id = getJobID(coflow_id)
        self.arrived_job_dict[job_id].completion_stage(coflow_id, update_time_stamp, self)
        return self.arrived_job_dict[job_id].run(self)
