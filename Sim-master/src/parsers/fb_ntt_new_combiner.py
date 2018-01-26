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
ORIG_GBPS = 250.0


def main():
    if len(sys.argv) >= 5:
        file_path = sys.argv[1]
        topo_path = sys.argv[2]
        topo_gbps = float(sys.argv[3])
        NUM_DC = int(sys.argv[4])
    else:
        print "Usage: ", sys.argv[0], "<path_to_fb_coflow_trace> <path_to_topology_data> <topo_gbps> <max_dc>"
        sys.exit(1)

    dc_locs = []
    start_reading = False
    is_dc = dict()
    id_to_dc = dict()
    with open(topo_path, "rw+") as f:
        for line in f:
            if "node" in line:
                start_reading = True
            if start_reading and "id " in line:
                idx = int(line.split()[1])
            if start_reading and "label" in line:
                splits = line.split()
                dc_loc = splits[1][1:-1]
                dc_locs.append(dc_loc)
                id_to_dc[idx] = dc_loc
            if start_reading and "dc" in line and "label" not in line:
                is_dc[dc_loc] = False
                if "True" in line:
                    is_dc[dc_loc] = True

    tot_bw = 0
    start_reading = False
    with open(topo_path, "rw+") as f:
        for line in f:
            if "edge" in line:
                start_reading = True
            if start_reading and "source " in line:
                source = id_to_dc[int(line.split()[1])]
            if start_reading and "target" in line:
                target = id_to_dc[int(line.split()[1])]
            if start_reading and "bandwidth" in line:
                bw = int(line.split()[1])
                if is_dc[source] and is_dc[target]:
                    tot_bw += bw
    sys.stderr.write("Total bandwidth from DC nodes = %d\n" % (tot_bw / 1024))

    fo = open(file_path, "rw+")
    lines = fo.readlines()
    fo.close()

    dc_locs = [dc_loc for dc_loc in dc_locs if is_dc[dc_loc]]
    sys.stderr.write("Number of datacenters in %s = %d\n" % (topo_path, len(dc_locs)))

    splits = lines[0].split()
    num_ports = int(splits[0])
    num_coflows = int(splits[1])

    for cid in range(1, num_coflows + 1):
        splits = lines[cid].split()

        coflow_id = splits[0]
        arrival_time = int(splits[1]) / 1000
        num_mappers = int(splits[2])
        num_reducers = int(splits[3 + num_mappers])
        reduce_locs = [splits[num_mappers + i + 4] for i in range(num_reducers)]
        shuffle_mb = 0.0
        for r in reduce_locs:
            rr = float(r.split(":")[1])
            shuffle_mb += rr
        if shuffle_mb == 0.0:
            shuffle_mb = 1

        num_mappers = int(math.ceil((1.0 * num_mappers / num_ports) * NUM_DC))
        num_reducers = int(math.ceil((1.0 * num_reducers / num_ports) * NUM_DC))

        # Print out one job at a time
        print "# JOB-%s" % coflow_id
        print 2, cid, arrival_time
        shuffle(dc_locs)
        print "Map1", num_mappers, " ".join(dc_locs[:num_mappers])
        shuffle(dc_locs)
        print "Reducer1", num_reducers, " ".join(dc_locs[:num_reducers])
        print "1"
        print "Map1 Reducer1", int(shuffle_mb)


if __name__ == "__main__":
    main()
