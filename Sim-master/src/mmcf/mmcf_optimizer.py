from pulp import *
from collections import defaultdict
from src.exceptions import OptimizerException
from subprocess import Popen, PIPE
import logging
import src.globals

sys.path.append("..")

# setup logger
logger = logging.getLogger(__name__)


def mmcf_optimize(coflow, net_graph, if_single_direction=False):
    if src.globals.isBaseline:
        return pulp_optimize(coflow, net_graph, if_single_direction)
    return glpk_optimize(coflow, net_graph)
    # return pulp_optimize(coflow, net_graph, if_single_direction)


def glpk_optimize(coflow, net_graph):
    path_root = "/tmp"
    # PATH_ROOT = "/Volumes/RAMDisk"
    mod_file_name = path_root + "/MinCCT.mod"

    # ---------- setup variables and constraints for linear programming ---------- #
    dat_string = "data;\n\n"

    int_to_nid = {}
    nid_to_int = {}
    nid = 0
    for i in net_graph.nodes:
        int_to_nid[nid] = i
        nid_to_int[i] = nid
        nid += 1

    dat_string += "set N:="
    for i in range(len(net_graph.nodes)):
        dat_string += " %d" % i
    dat_string += ";\n"

    flow_dict = coflow.flow_dict
    flow_id_list = sorted(flow_dict.keys())

    dat_string += "set F:="
    for fid in flow_id_list:
        dat_string += " f%d" % (fid - 1)
    dat_string += ";\n\n"

    dat_string += "param b:\n"
    for i in range(len(net_graph.nodes)):
        dat_string += "%d " % i
    dat_string += ":=\n"
    for i in range(len(net_graph.nodes)):
        dat_string += "%d " % i
        for j in range(len(net_graph.nodes)):
            if i == j or net_graph.link_dict.get(start_node_id=int_to_nid[i], end_node_id=int_to_nid[j]) is None:
                dat_string += " 0.000"
            else:
                dat_string += " %.3f" % float(net_graph.link_dict.get(start_node_id=int_to_nid[i],
                                                                      end_node_id=int_to_nid[j]).bandwidth)
        dat_string += "\n"
    dat_string += ";\n\n"

    dat_string += "param fs:=\n"
    for fid in flow_id_list:
        dat_string += " f%d %d\n" % (fid - 1, nid_to_int[flow_dict[fid].source_nid])
    dat_string += ";\n\n"

    dat_string += "param fe:=\n"
    for fid in flow_id_list:
        dat_string += " f%d %d\n" % (fid - 1, nid_to_int[flow_dict[fid].sink_nid])
    dat_string += ";\n\n"

    dat_string += "param fv:=\n"
    for fid in flow_id_list:
        dat_string += " f%d %.3f\n" % (fid - 1, flow_dict[fid].volume)
    dat_string += ";\n\n"

    dat_string += "end;\n"

    dat_file_name = path_root + "/%s.dat" % coflow.id
    with open(dat_file_name, "w") as text_file:
        text_file.write(dat_string)

    # ---------- solve the linear programming problem ---------- #

    out_file_name = path_root + "/%s.out" % coflow.id
    cmd = ['glpsol -m %s -d %s -o %s' % (mod_file_name, dat_file_name, out_file_name)]
    process = Popen(cmd, shell=True, stdout=PIPE)
    process.wait()

    # ---------- set up return value ---------- #
    flow_kv = defaultdict(dict)
    missing_pieces = False
    with open(out_file_name, "r") as text_file:
        for line in text_file:
            if "Objective" in line:
                alpha = float(line.split()[3])
                if alpha < 0.00001:
                    raise OptimizerException('given coflow cannot be allocated on current network')
                else:
                    completion_time = 1. / alpha
            elif "f[f" in line and "NL" not in line:
                splits = line.split()
                fsplits = splits[1][3:-1].split(",")
                fi = int(fsplits[0])
                fs = int_to_nid[int(fsplits[1])]
                fe = int_to_nid[int(fsplits[2])]
                try:
                    bw = round(abs(float(splits[3])), 2)
                    if bw >= round(0.01, 2) and fs != fe:
                        flow_kv[fi + 1][(fs, fe)] = bw
                    missing_pieces = False
                except Exception as e:
                    missing_pieces = True
            elif "f[f" not in line and "NL" not in line and missing_pieces:
                splits = line.split()
                try:
                    bw = round(abs(float(splits[1])), 2)
                    if bw >= round(0.01, 2) and fs != fe:
                        flow_kv[fi + 1][(fs, fe)] = bw
                    missing_pieces = False
                except Exception as e:
                    missing_pieces = True
            elif "alpha" in line:
                missing_pieces = False

    return completion_time, flow_kv


def pulp_optimize(coflow, net_graph, if_single_direction=False):
    flow_dict = coflow.flow_dict
    # ---------- setup variables for linear programming ---------- #
    # alpha represents the relative speed of flow, the reciprocal of completion time
    alpha = LpVariable("alpha", lowBound=0, cat='Continuous')
    # flux represents the optimal bandwidth that a flow should take from a link
    flux = {}
    for flow_id in flow_dict:
        flux_matrix = defaultdict(dict)
        for node_i in net_graph.nodes:
            for node_j in net_graph.nodes:
                if net_graph.link_dict.get(start_node_id=node_i, end_node_id=node_j) is not None:
                    flux_matrix[node_i][node_j] = \
                        LpVariable("f_"+node_i+"_"+node_j+"_"+str(flow_id), lowBound=0, cat='Continuous')
        flux[flow_id] = flux_matrix

    # ---------- solve the linear programming problem ---------- #
    lp = LpProblem("multi commodity flow problem", LpMaximize)
    # flux conservation and source/sink demand
    flux_conservation_constrain(lp, alpha, flux, flow_dict, net_graph)
    # capacity constrain
    if if_single_direction is True:
        single_link_capacity_constrain(lp, flux, flow_dict, net_graph)
    else:
        bidirectional_link_capacity_constrain(lp, flux, flow_dict, net_graph)
    lp += alpha  # maximize the rate, minimize transfer time
    LpSolverDefault.msg = 0
    status = lp.solve()
    # print LpStatus[status]
    # print_flux(flux)

    # ---------- set up return value ---------- #
    if value(alpha) < 0.00001:
        raise OptimizerException('given coflow cannot be allocated on current network')
    else:
        completion_time = 1. / value(alpha)

    flow_kv = defaultdict(dict)
    for flow_id in flow_dict:
        for node_i, flux_matrix_row in flux[flow_id].items():
            for node_j, flux_value in flux_matrix_row.items():
                if round(abs(value(flux_value)), 2) >= round(0.01, 2):
                    flow_kv[flow_id][(node_i, node_j)] = round(value(flux_value), 2)
                elif abs(value(flux_value)) != 0:
                    logger.debug("warning: ignored small flux_value for flow: " + str(flow_id)
                                 + " on link: " + "(" + str(node_i) + ", " + str(node_i) + ")")
                    # flow_kv[flow_id][(node_i, node_j)] = 0

    logger.info('MMCF result for coflow ' + str(coflow.id) + '\n'
                + '\tCompletion time:' + str(completion_time)
                + log_flow_assignment(flow_kv, "\n\tAssigned flow kv"))

    return completion_time, flow_kv


# --------------- constrains for linear programming --------------- #
def bidirectional_link_capacity_constrain(lp, flux, flow_dict, net_graph):
    for node_i in net_graph.nodes:
        for node_j in net_graph.nodes:
            if net_graph.link_dict.get(start_node_id=node_i, end_node_id=node_j) is not None:
                lp += \
                    lpSum(flux[flow_id][node_i][node_j] for flow_id in flow_dict) \
                    <= net_graph.link_dict.get(start_node_id=node_i, end_node_id=node_j).bandwidth
    return None


def single_link_capacity_constrain(lp, flux, flow_dict, net_graph):
    for node_i in net_graph.nodes:
        for node_j in net_graph.nodes:
            if net_graph.link_dict.get(start_node_id=node_i, end_node_id=node_j) is not None:
                lp += \
                    lpSum(flux[flow_id][node_i][node_j] + flux[flow_id][node_j][node_i] for flow_id in flow_dict) \
                    <= net_graph.link_dict.get(start_node_id=node_i, end_node_id=node_j).bandwidth
    return None


def flux_conservation_constrain(lp, alpha, flux, flow_dict, net_graph):
    for flow_id in flow_dict:
        flux_matrix = flux[flow_id]
        source_nid = flow_dict[flow_id].source_nid
        sink_nid = flow_dict[flow_id].sink_nid
        for start_node, flux_matrix_row in flux_matrix.items():
            # flux conservation & demand satisfaction
            if start_node == source_nid:
                for end_node in flux_matrix_row:  # constrains for source node
                    lp += flux_matrix[end_node][start_node] == 0
                lp += lpSum(flux_matrix[start_node][end_node] for end_node in flux_matrix_row) \
                    == flow_dict[flow_id].volume * alpha
            elif start_node == sink_nid:
                for end_node in flux_matrix_row:  # constrains for sink node
                    lp += flux_matrix[start_node][end_node] == 0
                lp += lpSum(flux_matrix[end_node][start_node] for end_node in flux_matrix_row) \
                    == flow_dict[flow_id].volume * alpha
            else:
                lp += lpSum(flux_matrix[start_node][end_node] for end_node in flux_matrix_row) - \
                    lpSum(flux_matrix[end_node][start_node] for end_node in flux_matrix_row) \
                    == 0
    return None


# --------------- print data structure for debugging --------------- #

def print_flux(flux):
    for flow_id, flux_matrix in flux.items():
        print("\nflow: " + str(flow_id))
        for node_i, node_i_neighbour in flux_matrix.items():
            for node_j, flux_value in node_i_neighbour.items():
                print str(value(flux_value)) + " ",
            print
    print


def log_flow_assignment(flow_kv, flow_kv_str):
    log_msg = str()
    log_msg += flow_kv_str + " = {\n"
    for flow_id, links in flow_kv.items():
        log_msg += "\t\t " + str(flow_id) + " : {\n"
        for link_node_tuple, bandwidth in links.items():
            log_msg += "\t\t\t( " + str(link_node_tuple[0]) + " , " \
                       + str(link_node_tuple[1]) + " ) : " + str(value(bandwidth)) + '\n'
        log_msg += "\t\t}\n"
    log_msg += "\t}\n"
    return log_msg
