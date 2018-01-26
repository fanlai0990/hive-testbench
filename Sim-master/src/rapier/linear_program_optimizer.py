from src.network.path import Path, Link  # deletable
from collections import defaultdict
from pulp import *


def minimize_coflow_completion_time(coflow):
    sorted_flow_keys = sort_flow_by_volume(coflow)
    links_table = map_flows_to_link(coflow)

    # set up the linear programming variables to find the routes for each flow
    alpha_relaxed = LpVariable("alpha", lowBound=0, cat='Continuous')
    m_flow_rate_dic = {}
    key_num = 1
    for key, flow in coflow.items():
        m_flow_rate = {}
        link_num = 1
        for link in flow[0]:
            variable_name_str = "m_" + str(key_num) + "_" + str(link_num)
            m_flow_rate[link] = LpVariable(variable_name_str, lowBound=0, cat='Continuous')
            link_num += 1
        m_flow_rate_dic[key] = m_flow_rate
        key_num += 1

    # solve linear programming problem to find the routes for each flow
    lp1 = LpProblem("routing_problem", LpMaximize)
    for key, m_flow_rate in m_flow_rate_dic.items():
        lp1 += lpSum(m_value for link, m_value in m_flow_rate.items()) >= alpha_relaxed
        lp1 += lpSum(m_value for link, m_value in m_flow_rate.items()) <= alpha_relaxed
    for link, flow_keys in links_table.items():
        lp1 += lpSum(coflow[key][1] * m_flow_rate_dic[key][link] for key in flow_keys) <= link.bandwidth
    lp1 += alpha_relaxed
    status1 = lp1.solve()
    print LpStatus[status1]

    # get the routing assignment plan
    link_assignment = link_assign(m_flow_rate_dic, sorted_flow_keys, links_table)

    # solve the linear programming problem to find the completion time
    alpha = LpVariable("alpha_time", lowBound=0, cat='Continuous')
    lp2 = LpProblem("scheduling problem", LpMaximize)
    for flow_key, link in link_assignment.items():
        lp2 += coflow[flow_key][1] * alpha <= link.bandwidth
    lp2 += alpha
    status2 = lp2.solve()
    print LpStatus[status2]
    coflow_completion_time = 1. / value(alpha)

    # print out results
    for key, m_flow_rate in m_flow_rate_dic.items():
        print "flow " + str(key)
        for link, m_value in m_flow_rate.items():
            print "link: %d, m: %f" % (link.bandwidth, value(m_value))
    print "alpha_relax: %f" % (value(alpha_relaxed))
    print "alpha: %f" % (value(alpha))

    return coflow_completion_time


# sort the flows in the coflow by the value of the volume
def sort_flow_by_volume(coflow_kv):
    sorted_list = []
    for key, flow in coflow_kv.items():
        sorted_list.append((key, flow[1]))
    sorted_list.sort(key=lambda tup: tup[1], reverse=True)
    return sorted_list


# get the flows which use a particular link
def map_flows_to_link(coflow_kv):
    link_table = defaultdict(list)
    for key, flow in coflow_kv.items():
        for link in flow[0]:
            link_table[link].append(key)
    return link_table


# make assignment
def link_assign(m_flow_rate_dic, sorted_flow_keys, link_table):
    # initiate the assignment container and mark all link as unused
    assignment = {}
    link_assigned = {}
    for link, flow_list in link_table.items():
        link_assigned[link] = False

    # assign link from the largest volume flow
    for key, volume in sorted_flow_keys:
        greatest_m_value = 0
        assigned_link = object()
        for link, m_value in m_flow_rate_dic[key].items():
            if (link_assigned[link] is False) and (value(m_value) >= greatest_m_value):
                greatest_m_value = value(m_value)
                assigned_link = link
        assignment[key] = assigned_link
        link_assigned[assigned_link] = True
    return assignment
