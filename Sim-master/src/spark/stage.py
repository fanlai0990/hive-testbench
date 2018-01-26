from random import choice
from src.network.flow import CoFlow

# Constant values
JOB_OPERATOR_DELIMITER = ":JOB:"
JOB_IDENTIFIER = "job"


def createUniqueName(job_id, other_id):
    return job_id + JOB_OPERATOR_DELIMITER + other_id


def getSeperateID(unqiue_name):
    names = unqiue_name.split(JOB_OPERATOR_DELIMITER)
    assert len(names) == 2
    return names


def getJobID(unqiue_name):
    return getSeperateID(unqiue_name)[0]


def getStageID(unique_name):
    return getSeperateID(unique_name)[1]


class Stage(object):
    INIT = "INIT"
    RUNNING = "RUNNING"
    READY = "READY"
    COMPLETE = "COMPLETE"
    WAIT = "WAIT"

    def __init__(self, job_id, stage_id, stage_type, task_locs, random_gen, net_graph):
        self.stage_id = stage_id
        self.unique_id = createUniqueName(job_id, stage_id)
        self.stage_type = stage_type
        self.task_locs = task_locs
        self.random_generator = random_gen
        self.net_graph = net_graph
        self.child_stages = set([])
        self.parent_stages = set([])
        self.volume = None
        self.net_node = None
        self.__coflow = None
        self.depend_stage_ids = None
        self.__status = Stage.INIT

    @property
    def status(self):
        if self.__status == Stage.WAIT:
            if self.is_ready():
                self.__status = Stage.READY
        return self.__status

    @property
    def coflow(self):
        if self.__coflow is None:
            start_end_volume_list = []
            sum_volume = 0
            for child_stage in self.child_stages:
                single_flow_volume = round(child_stage.volume / (len(child_stage.task_locs) * len(self.task_locs)), 2)
                for child_task_loc in child_stage.task_locs:
                    for parent_task_loc in self.task_locs:
                        if child_task_loc == parent_task_loc:
                            # if happens to be the same location, re-assign the child loc
                            # child_task_loc = self.choose_different_node(net_node=child_task_loc)
                            # if happens to be the same location, skip the flow instead of finding a new one
                            continue
                        start_end_volume_list.append(((child_task_loc, parent_task_loc), single_flow_volume))
                        sum_volume += single_flow_volume
            if len(start_end_volume_list) > 0:
                self.__coflow = CoFlow(volume=sum_volume, id=self.unique_id, start_end_volume_list=start_end_volume_list)
            else:
                self.__status = Stage.COMPLETE  # TODO: choose_different_node related
                for parent_stage in self.parent_stages:
                    parent_stage.undepend(self.stage_id)
        return self.__coflow

    def choose_different_node(self, net_node):
        net_nodes = self.net_graph.nodes.keys()
        net_nodes.remove(net_node)
        return self.random_generator.choice(net_nodes)

    def build(self):
        self.depend_stage_ids = map(lambda x: x.stage_id, self.child_stages)
        self.__status = Stage.WAIT

    def is_ready(self):
        if self.__status == Stage.READY:
            return True
        if self.depend_stage_ids is None:
            assert self.__status == Stage.INIT
            return False
        if len(self.depend_stage_ids) == 0:
            return True
        else:
            assert self.__status == Stage.WAIT
            return False

    def run(self):
        # print "try to run stage: ", self.unique_id
        # print self.__str__()
        assert self.status == Stage.READY, self.status
        self.__status = Stage.RUNNING

    def undepend(self, stage_id):
        # print "try to undepend stage: %s from stage: %s" %(self.stage_id, stage_id)
        # print self.__str__()
        assert self.status == Stage.WAIT
        if stage_id in self.depend_stage_ids:
            self.depend_stage_ids.remove(stage_id)

    def complete(self):
        # print "completing stage: ", self.stage_id
        # print self.__str__()
        assert self.status == Stage.RUNNING
        self.__status = Stage.COMPLETE
        for parent_stage in self.parent_stages:
            parent_stage.undepend(self.stage_id)

    def add_parent(self, parent_stage):
        self.parent_stages.add(parent_stage)

    def add_child(self, child_stage):
        self.child_stages.add(child_stage)

    def __str__(self):
        parent_stage_ids = map(lambda stage: stage.stage_id, self.parent_stages)
        child_stage_ids = map(lambda stage: stage.stage_id, self.child_stages)
        return "----- STAGE -----\n" \
               + "stage_id: \t\t%s\n" % self.stage_id \
               + "volume: \t\t%s\n" % str(self.volume) \
               + "net_node: \t\t%s\n" % self.net_node \
               + "status: \t\t%s\n" % self.status \
               + "parent_stages: \t\t%s\n" % ", ".join(parent_stage_ids) \
               + "child_stages: \t\t%s\n" % ", ".join(child_stage_ids) \
               + "depends: \t\t%s" % ", ".join(self.depend_stage_ids)
