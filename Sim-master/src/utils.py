# -*- coding: utf-8 -*-
import json
from network.flow import CoFlow

from random import randint
import textwrap


def readJson(fileName):
    with open(fileName) as f:
        return json.load(f)


def read_source2net_from_net_node_data(net_node_data):
    source2net = {}
    for net_node_id, value in net_node_data:
        if len(value['dataSources']) > 0:
            source2net[net_node_id] = value['dataSources']
    return source2net

def make_shuffle_coflow(shuffle_dict, id):
    start_end_volume_list = []
    for start_nid, volume in shuffle_dict['start_nodes'].items():
        for end_nid in shuffle_dict['end_nodes']:
            start_end_volume_list.append(((start_nid, end_nid), volume))
    return CoFlow(volume=shuffle_dict["all_volume"], id=id, start_end_volume_list=start_end_volume_list)


def make_arbitrary_coflow(arbitrary_dict, id):
    start_end_volume_list = []
    for start_nid in arbitrary_dict['flow_descriptor']:
        for end_nid, volume in arbitrary_dict['flow_descriptor'][start_nid].items():
            start_end_volume_list.append(((start_nid, end_nid), volume))
    return CoFlow(volume=arbitrary_dict['all_volume'], id=id, start_end_volume_list=start_end_volume_list)


def make_coflow_dict(fileName):
    coflow_dict = dict()
    data = readJson(fileName)
    coflow_descriptor_dict = data["CoFlow"]
    for coflow_id, coflow_descriptor in coflow_descriptor_dict.items():
        coflow_id = int(coflow_id)
        if coflow_descriptor['type'] == 'shuffle':
            coflow_dict[coflow_id] = make_shuffle_coflow(coflow_descriptor, coflow_id)
        elif coflow_descriptor['type'] == 'arbitrary':
            coflow_dict[coflow_id] = make_arbitrary_coflow(coflow_descriptor, coflow_id)
    return coflow_dict


class stride(object):
    def __init__(self):
        self.name = "stride"
        self.cluster_to_pass = dict()

    def initialize(self, plan):
        for (c, value) in plan.items():
            self.cluster_to_pass[c] = value.dist

    def find(self, partial_res):
        res = None
        if len(partial_res) == 1:
            res = partial_res[0]
        elif len(partial_res) > 1:
            res = reduce(lambda x, y: y if self.cluster_to_pass[x] > self.cluster_to_pass[y] else x, partial_res)
        else:
            raise Exception("Does not found partitions in DCs")
        return res

    def update(self, res, plan, max_intersect):
        # update stride pass
        self.cluster_to_pass[res] += plan[res].dist * len(max_intersect)

class nearest(object):
    def __init__(self):
        self.name = "nearest"
        self.cluster_to_dist = dict()

    def initialize(self, plan):
        for (c, value) in plan.items():
            self.cluster_to_dist[c] = value.dist

    def find(self, partial_res):
        res = None
        if len(partial_res) == 1:
            res = partial_res[0]
        elif len(partial_res) > 1:
            res = reduce(lambda x, y: y if self.cluster_to_dist[x] > self.cluster_to_dist[y] else x, partial_res)
        else:
            raise Exception("Does not found partitions in DCs")
        return res

    def update(self, res, plan, max_intersect):
        # nothing to update
        pass

class rand(object):
    def __init__(self):
        self.name = "rand"

    def initialize(self, plan):
        # nothing to init
        pass

    def find(self, partial_res):
        res = None
        
        idx = randint(0,len(partial_res)-1)
        res = partial_res[idx]
        
        return res

    def update(self, res, plan, max_intersect):
        # nothing to update
        pass

class formater(object):
    def __init__(self, width=120):
        self.wrapper = textwrap.TextWrapper(width=width)

    def wrap(self, str):
        return "\n".join(self.wrapper.wrap(str))
