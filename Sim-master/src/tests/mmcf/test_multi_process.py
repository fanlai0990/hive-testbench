from multiprocessing import Pool
from src.network import CoFlow
from src.mmcf import mmcf_optimize
from src.utils import readJson
from src.network import NetGraph
from itertools import repeat

import os
import random
import time


def mmcf_wrapper(coflow_graph_tuple):
    cct, flow_link_bw = mmcf_optimize(coflow=coflow_graph_tuple[0], net_graph=coflow_graph_tuple[1])

    sd = coflow_graph_tuple[2]
    time.sleep(sd)
    for i in range(sd * 1000000):
        None

    print "Finished Coflow", coflow_graph_tuple[0].id, " with delay = ", sd

    return cct, flow_link_bw


def make_net_graph(data_name='simple'):
    test_file = readJson(os.path.dirname(__file__) + '/../scheduler/net_data.json')
    test_data = test_file[data_name]
    return NetGraph(linkData=test_data['NetGraph'], nodesData=test_data['NetNode'])


if __name__ == '__main__':
    net_graph = make_net_graph()
    flow_data = [
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n3', 'n4'), 10), (('n3', 'n4'), 10), (('n1', 'n4'), 5)],
                 [(('n1', 'n2'), 10), (('n3', 'n4'), 95)],
                 [(('n1', 'n2'), 10), (('n3', 'n4'), 95)],
                 [(('n1', 'n2'), 10), (('n3', 'n4'), 95)],
                ]
    coflow_list = list()
    for i in range(len(flow_data)):
        coflow_list.append(CoFlow(volume=sum(flow_data[i][j][1] for j in range(len(flow_data[i]))),
                                  start_end_volume_list=flow_data[i],
                                  id=i))

    rands = list()
    for i in coflow_list:
        rands.append(random.randint(1, 10))

    start = time.time()
    pool = Pool(processes=4)
    result_list = pool.map(mmcf_wrapper, zip(coflow_list, repeat(net_graph), rands))
    # for flow_kv in result_list:
    #     print 'cct', flow_kv[0]
    #     print 'flow_link_bw',
    #     log_msg = " = {\n"
    #     for flow_id, links in flow_kv[1].items():
    #         log_msg += "\t\t " + str(flow_id) + " : {\n"
    #         for link_node_tuple, bandwidth in links.items():
    #             log_msg += "\t\t\t( " + str(link_node_tuple[0]) + " , " \
    #                        + str(link_node_tuple[1]) + " ) : " + str(bandwidth) + '\n'
    #         log_msg += "\t\t}\n"
    #     log_msg += "\t}\n"
    #     print log_msg
    print "Parallel took", time.time() - start, "\n"

    start = time.time()
    for c in range(len(coflow_list)):
        cct, flow_link_bw = mmcf_optimize(coflow=coflow_list[c], net_graph=net_graph)

        sd = rands[c]
        time.sleep(sd)
        counter = 0
        for i in range(sd * 1000000):
            counter += 1

        print "Finished Coflow", coflow_list[c].id, " with delay = ", sd
    print "Serial took", time.time() - start
