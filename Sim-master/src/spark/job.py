from . import Stage
from .stage import getSeperateID
import logging

# setup logger
logger = logging.getLogger(__name__)


class JobDAG(object):
    def __init__(self, job_id, stage_dict, vertexs, net_graph, timestamp=0):
        self.job_id = job_id
        self.start_time = timestamp
        self.timestamp = timestamp
        self.net_graph = net_graph
        self.stage_dict = stage_dict  # hold for all stagess
        self.vertexs = vertexs  # pair of stage_ids, child -> parent
        self.waiting_stage_dict = {}
        self.completed_stage_dict = {}
        self.ready_stage_dict = {}
        self.running_stage_dict = {}
        self.assign_vertixes()
        self.remove_dangling_stages()
        self.build_stages()
        roots = filter(lambda x: len(x.parent_stages) == 0, self.stage_dict.values())
        assert len(roots) == 1, "job_id: " + job_id  # always have one root for now from observation
        self.root = roots[0]
        self.top_sorted_stages = self.topology_sort_stages()
        self.classify_stages()
        self.release_begin_stages()

    def remove_dangling_stages(self):
        '''
        From observation, we know there might be cases where a stage is not connected to any other stages
        One guess is that those are due to Map failure. We remove those stages for simplicity.
        '''
        for stage_id, stage in self.stage_dict.items():
            if len(stage.parent_stages) == 0 and len(stage.child_stages) == 0:
                del self.stage_dict[stage_id]

    def build_stages(self):
        map(lambda x: x.build(), self.stage_dict.values())

    def assign_vertixes(self):
        for vertex in self.vertexs:
            # assert len(vertex) == 2
            child = self.stage_dict[vertex[0]]
            parent = self.stage_dict[vertex[1]]
            child.add_parent(parent)
            parent.add_child(child)

            # If data_size is defined, put that into child volume for transmission
            if len(vertex) == 3:
                data_size = vertex[2]
                child.volume = data_size

    def print_stages(self):
        def function(stage):
            # print stage
            logger.info('\n' + stage.__str__())
        self.stage_recursive_dispatcher(function=function, stage=self.root, pre_order=True)

    def release_begin_stages(self):
        for stage in self.top_sorted_stages:
            if len(stage.child_stages) == 0:
                assert stage.stage_id in self.ready_stage_dict
                stage.run()
                stage.complete()
                self.completed_stage_dict[stage.stage_id] = stage
                del self.ready_stage_dict[stage.stage_id]
        self.classify_stages()

    @staticmethod
    def stage_recursive_dispatcher(function, stage, pre_order=False):
        if pre_order:
            function(stage)
        for child_stage in stage.child_stages:
            JobDAG.stage_recursive_dispatcher(function, child_stage, pre_order)
        if not pre_order:
            function(stage)

    def remove_from_status_dict(self, stage_id):
        if stage_id in self.completed_stage_dict:
            del self.completed_stage_dict[stage_id]
        if stage_id in self.ready_stage_dict:
            del self.ready_stage_dict[stage_id]
        if stage_id in self.waiting_stage_dict:
            del self.waiting_stage_dict[stage_id]
        if stage_id in self.running_stage_dict:
            del self.running_stage_dict[stage_id]

    def classify_stage(self, stage):
        stage_id = stage.stage_id
        if stage.status is Stage.READY:
            self.ready_stage_dict[stage_id] = stage
        elif stage.status == Stage.WAIT:
            self.waiting_stage_dict[stage_id] = stage
        elif stage.status == Stage.RUNNING:
            self.ready_stage_dict[stage_id] = stage
        elif stage.status == Stage.COMPLETE:
            self.completed_stage_dict[stage_id] = stage

    def classify_stages(self):
        for stage_id, stage in self.stage_dict.items():
            self.remove_from_status_dict(stage_id)
            self.classify_stage(stage)

    def topology_sort_stages(self):
        def dfs(stage):
            if stage not in check_set:
                for child_stage in stage.child_stages:
                    dfs(child_stage)
                check_set.add(stage)
                result_list.append(stage)

        result_list = []
        check_set = set()
        dfs(self.root)
        return result_list  # top down sort

    def run(self, mock_spark):
        coflow_dict = {}
        for stage_id, stage in self.ready_stage_dict.items():
            stage.run()
            self.running_stage_dict[stage_id] = stage
            del self.ready_stage_dict[stage_id]
            coflow_dict[stage.unique_id] = stage.coflow
            if coflow_dict[stage.unique_id] is None:
                if stage.stage_id == self.root.stage_id and stage.status == stage.COMPLETE:
                    print '@time %.4f' % self.timestamp + \
                          ' Job ' + str(self.job_id) + \
                          '\tStartTime %.4f' % self.start_time + \
                          ' JobCompletionTime %.4f' % (self.timestamp - self.start_time)
                    mock_spark.job_completion_time_dict[self.job_id] = (self.start_time, self.timestamp)
                del coflow_dict[stage.unique_id]  # TODO: choose_different_node related
        return coflow_dict

    def completion_stage(self, unique_id, update_time_stamp, mock_spark):
        self.timestamp = update_time_stamp
        job_id, stage_id = getSeperateID(unique_id)
        assert(job_id == self.job_id)
        stage = self.running_stage_dict[stage_id]
        self.completed_stage_dict[stage_id] = stage
        del self.running_stage_dict[stage_id]
        stage.complete()
        for waiting_stage_id, waiting_stage in self.waiting_stage_dict.items():
            if waiting_stage.status == Stage.READY:
                self.ready_stage_dict[waiting_stage_id] = waiting_stage
                del self.waiting_stage_dict[waiting_stage_id]

        if stage_id == self.root.stage_id:
            print '@time %.4f' % self.timestamp + \
                  ' Job ' + str(self.job_id) + \
                  '\tStartTime %.4f' % self.start_time + \
                  ' JobCompletionTime %.4f' % (self.timestamp - self.start_time)
            mock_spark.job_completion_time_dict[self.job_id] = (self.start_time, self.timestamp)
