from random import shuffle
import math
import sys


# Scale for flow volume in order to shorten simulation time
VOLUME_SCALE = 1

# Maximum tasks per stage
# BigBench: 1009
# TPC-DS: 318
# TPC-H: 98

# Original bandwidth
ORIG_GBPS = 300.0


def main():
    if len(sys.argv) >= 6:
        file_path = sys.argv[1]
        max_tasks = int(sys.argv[2])
        topo_path = sys.argv[3]
        topo_gbps = float(sys.argv[4])
        NUM_DC = int(sys.argv[5])
    else:
        print "Usage: ", sys.argv[0], "<path_to_benchmark_trace> <max_tasks> <path_to_topology_data> <topo_gbps> <max_dc> [path_to_arrival_dist]"
        sys.exit(1)

    a_id = 0
    arrival_times_dist = []
    if len(sys.argv) >= 7:
        arrival_dist_path = sys.argv[6]
        fo = open(arrival_dist_path, "rw+")
        arrival_times_dist = [int(x) for x in fo.readlines()]
        fo.close()

    dc_locs = []
    start_reading = False
    with open(topo_path, "rw+") as f:
        for line in f:
            if "node" in line:
                start_reading = True
            if start_reading and "label" in line:
                splits = line.split()
                dc_locs.append(splits[1][1:-1])

    fo = open(file_path, "rw+")
    lines = fo.readlines()
    fo.close()

    sys.stderr.write("Number of datacenters in %s = %d\n" % (topo_path, len(dc_locs)))

    lid = 0
    while lid < len(lines):
        comment_to_print = lines[lid]
        if lines[lid].startswith("#"):
            lid += 1

        splits = lines[lid].split()
        lid += 1
        num_stages = int(splits[0])
        job_id = splits[1]
        if len(arrival_times_dist) > 0:
            arrival_time = arrival_times_dist[a_id]
            a_id += 1
        else:
            arrival_time = int(splits[2])
        counters_to_print = "%d %s %d\n" % (num_stages, job_id, arrival_time)

        stage_vol_dic = {}
        stage_tasks_dic = {}
        vertices_to_print = []
        for x in range(num_stages):
            splits = lines[x + lid].split()
            stage_name = splits[0]
            avg_dur = float(splits[1])
            n_in = float(splits[4])
            n_out = float(splits[5])
            num_tasks = int(splits[8])

            # Total network activity of this stage
            vol = avg_dur * (n_in + n_out) * num_tasks * 1024 / 8 * VOLUME_SCALE

            num_tasks = int(math.ceil((1.0 * num_tasks / max_tasks) * NUM_DC))

            stage_vol_dic[stage_name] = vol
            stage_tasks_dic[stage_name] = num_tasks

            shuffle(dc_locs)
            vertices_to_print.append(stage_name + " " + str(num_tasks) + " " + " ".join(dc_locs[:num_tasks]))
        lid += num_stages

        splits = lines[lid].split()
        lid += 1
        num_edges = int(splits[0])

        # First pass to determine ratio of split
        stage_parent_tasks_dic = {}
        for x in range(num_edges):
            splits = lines[x + lid].split()
            if splits[1] not in stage_parent_tasks_dic:
                stage_parent_tasks_dic[splits[1]] = 0
            stage_parent_tasks_dic[splits[1]] = stage_parent_tasks_dic[splits[1]] + stage_tasks_dic[splits[0]]

        # Second pass to actually split
        edges_to_print = []
        for x in range(num_edges):
            splits = lines[x + lid].split()
            vol = (stage_vol_dic[splits[0]] + stage_vol_dic[splits[1]]) / 2 * stage_tasks_dic[splits[0]] / stage_parent_tasks_dic[splits[1]]
            vol = int(vol / (ORIG_GBPS / topo_gbps))
            if vol == 0:
                vol = 1
            edges_to_print.append(splits[0] + " " + splits[1] + " " + str(vol) + '\n')
        lid += num_edges
        num_edges = len(edges_to_print)

        if num_stages > 0 and num_edges > 0:
            print comment_to_print,
            print counters_to_print,
            for x in range(num_stages):
                print vertices_to_print[x]
            print num_edges
            for x in range(num_edges):
                print edges_to_print[x],


if __name__ == "__main__":
    main()
