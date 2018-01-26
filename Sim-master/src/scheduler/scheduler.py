import sys
import traceback
# from multiprocessing import Pool
from pathos.multiprocessing import ProcessingPool as Pool

from src.mmcf import mmcf_optimize
from src.exceptions import OptimizerException, SchedulerException, AllocationException

import logging

# setup logger
logger = logging.getLogger(__name__)


class Scheduler(object):
    scheduler_type = 'Scheduler'

    def __init__(self, net_graph, coflow_dict, optimizer=None):
        self.unscheduled_coflow_dict = coflow_dict
        self.scheduled_coflow_dict = {}
        self.net_graph = net_graph
        self.optimizer = optimizer or mmcf_optimize
        self.coflow_completion_time_dict = dict()
        self.sorted_coflow = dict()
        self.minimum_id = -1
        self.maximum_id = -1
        self.unresolved_coflows = []

    # wrapper always assume bidirectional link
    @staticmethod
    def mmcf_wrapper(coflow_graph_tuple):
        try:
            cct, flow_kv = mmcf_optimize(coflow=coflow_graph_tuple[0], net_graph=coflow_graph_tuple[1])
            return cct, flow_kv
        except OptimizerException as e:
            return float('inf'), dict()

    def sort_coflows(self):
        # self.sort_coflows_parallel()
        self.sort_coflows_serial()


    def sort_coflows_parallel(self):
        # print "Number of unscheduled coflow", len(self.unscheduled_coflow_dict.items())
        logger.info("Number of unscheduled coflow " + str(len(self.unscheduled_coflow_dict.items())))
        for coflow_id, coflow in self.unscheduled_coflow_dict.items():
            # print coflow.print_flow_dict()
            logger.info("Unscheduled coflow info\n" + coflow.print_flow_dict())
        # print 'Sorting completion time of coflows'
        logger.debug('Sorting completion time of coflows')

        wrapper_input_list = list()
        for coflow_id, coflow in self.unscheduled_coflow_dict.items():
            wrapper_input_list.append((coflow, self.net_graph))
        worker_pool = Pool()
        wrapper_output_list = worker_pool.map(self.mmcf_wrapper, wrapper_input_list)  # [(cct, allocation)]
        assert len(wrapper_input_list) == len(wrapper_output_list)

        self.coflow_completion_time_dict.clear()
        for i in range(len(wrapper_input_list)):
            coflow = wrapper_input_list[i][0]
            cct = wrapper_output_list[i][0]
            flow_kv = wrapper_output_list[i][1]
            if cct != float('inf') and len(flow_kv) != 0:
                self.coflow_completion_time_dict[coflow.id] = cct
                coflow.remain_completion_time = cct
                coflow.flow_kv = flow_kv
            else:
                self.unresolve_coflow(coflow.id)

        self.sorted_coflow = sorted(self.coflow_completion_time_dict.items(), key=lambda x: x[1])
        if len(self.sorted_coflow) == 0:
            self.minimum_id = -1

    def sort_coflows_serial(self):
        coflow_completion_time_dict = dict()

        # print "Number of unscheduled coflow", len(self.unscheduled_coflow_dict.items())
        logger.info("Number of unscheduled coflow " + str(len(self.unscheduled_coflow_dict.items())))
        for coflow_id, coflow in self.unscheduled_coflow_dict.items():
            # print coflow.print_flow_dict()
            logger.info("Unscheduled coflow info\n" + coflow.print_flow_dict())
        # print 'Sorting completion time of coflows'
        logger.debug('Sorting completion time of coflows')

        for coflow_id, coflow in self.unscheduled_coflow_dict.items():
            try:
                completion_time, flow_kv = self.optimizer(coflow, self.net_graph)
                if len(flow_kv) == 0:
                    raise OptimizerException
                coflow_completion_time_dict[coflow_id] = completion_time
                coflow.remain_completion_time = completion_time
                coflow.flow_kv = flow_kv
            except OptimizerException as e:
                # print "OptimizerException: " + str(e) + ", current coflow id: " + str(coflow_id)
                logger.info("OptimizerException: " + str(e) + ", current coflow id: " + str(coflow_id))
                self.unresolve_coflow(coflow_id)
        self.coflow_completion_time_dict = coflow_completion_time_dict
        self.sorted_coflow = sorted(self.coflow_completion_time_dict.items(), key=lambda x: x[1])
        if len(self.sorted_coflow) == 0:
            self.minimum_id = -1

    def allocate_minimum(self):
        try:
            self.minimum_id = self.sorted_coflow[0][0]
            tmp_cct = self.sorted_coflow[0][1]
            self.allocate_coflow(self.minimum_id)
            del self.sorted_coflow[0]
            return self.minimum_id, tmp_cct
        except IndexError:
            self.minimum_id = -1
            raise AllocationException("AllocationException: there is no coflow can be allocated")
        except Exception:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb)  # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}'.format(line, text))
            exit(1)

    def allocate_maximum(self):
        try:
            self.maximum_id = self.sorted_coflow[-1][0]
            self.allocate_coflow(self.maximum_id)
        except Exception as e:
            self.maximum_id = -1
            raise AllocationException("Allocation error due to: " + str(e))

    def allocate_coflow(self, id):
        print "Coflow allocate " + str(id)
        logger.info("Allocate coflow " + str(id) + " on current network")
        self.unscheduled_coflow_dict[id].make(net_graph=self.net_graph)
        self.schedule_coflow(id)

    def unresolve_coflow(self, id):
        if id == -1:
            return
        self.unresolved_coflows.append(self.unscheduled_coflow_dict[id])
        del self.unscheduled_coflow_dict[id]

    def schedule_coflow(self, id):
        # print "Schedule coflow ", id
        logger.info("Schedule coflow " + str(id))
        self.scheduled_coflow_dict[id] = self.unscheduled_coflow_dict[id]
        del self.unscheduled_coflow_dict[id]

    def remove_coflow(self, coflow=None, id=None):
        if id is None:
            id = coflow.id
        if id in self.unscheduled_coflow_dict:
            del self.unscheduled_coflow_dict[id]
        elif id in self.scheduled_coflow_dict:
            del self.scheduled_coflow_dict[id]
        else:
            raise Exception('Remove a not existing coflow id %s' % str(id))

    def reschedule_coflow(self, coflow):
        self.remove_coflow(coflow=coflow)
        self.unscheduled_coflow_dict[coflow.id] = coflow
        coflow.release()

    @staticmethod
    def coflows2coflow_dict(coflows):
        coflow_dict = dict()
        for coflow in coflows:
            coflow_dict[coflow.id] = coflow
        return coflow_dict

    def schedule(self):
        raise Exception('Unimplemented')

    def remaining_bandwidth(self, if_print=False):
        bandwidth_info_str = str()

        bandwidth_sum = 0
        for links, bandwidth in self.net_graph.link_dict.get_link_kv().items():
            bandwidth_info_str += '\tlinks: (' + str(links[0]) + ', ' + str(links[1]) + ') bandwidth: ' + str(bandwidth) + '\n'
            bandwidth_sum += bandwidth

        if if_print:
            print "Remaining bandwidth on current network: \n" + bandwidth_info_str
        logger.info("Remaining bandwidth on current network: \n" + bandwidth_info_str)

        return bandwidth_sum
