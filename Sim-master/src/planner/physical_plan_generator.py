# -*- coding: utf-8 -*-
from copy import deepcopy as deep_copy
from physical_plan import PhysicalPlan, PhysicalNode, SOURCE, REDUCE, MAP, COMBINE
from logical_plan import SINK

class PhysicalPlanGenerator(object):
    """
    Generate corresponding physical plans
    """
    def __init__(self, base_physical_plan):
        self._cur_plan = base_physical_plan
        self._plans = [deep_copy(self._cur_plan)]

    @property
    def plans(self):
        assert len(self._plans) > 0
        return self._plans

    def generate(self):
        self.merge_combines()

    def merge_combines(self):
        while (len(self._cur_plan.root.sub_nodes) == 1):
            if self._cur_plan.merge_first_node_under_root(COMBINE):
                self._plans.append(deep_copy(self._cur_plan))
            else:
                return
