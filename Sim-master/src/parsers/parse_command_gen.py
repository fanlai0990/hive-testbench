topo_to_gbps = dict()
topo_to_gbps["swan"] = 35
topo_to_gbps["gb4"] = 42.5
topo_to_gbps["AttMpls"] = 56

topo_to_dc = dict()
topo_to_dc["swan"] = 3
topo_to_dc["gb4"] = 7
topo_to_dc["AttMpls"] = 11

workload_max_tasks = dict()
workload_max_tasks["BigBench"] = 1009
workload_max_tasks["TPC-DS"] = 318
workload_max_tasks["TPC-H"] = 98

workload_raw = dict()
workload_raw["BigBench"] = "queries_bb_FB_distr.txt"
workload_raw["TPC-DS"] = "queries_tpcds_FB_distr_new.txt"
workload_raw["TPC-H"] = "queries_tpch_FB_distr.txt"

for t in topo_to_gbps:
    for w in workload_max_tasks:
        print "python src/parsers/benchmark_topology_combiner.py data/raw_traces/%s %d data/gml/%s.gml %f %d data/FB_Arrival.txt > %s-%s.txt" % (workload_raw[w], workload_max_tasks[w], t, topo_to_gbps[t], topo_to_dc[t], w, t)

fb_raw = dict()
fb_raw["FB"] = "FB2010-1Hr-150-0.txt"
for t in topo_to_gbps:
    for w in fb_raw:
        print "python src/parsers/fb_topology_combiner.py data/raw_traces/%s data/gml/%s.gml %f %d > %s-%s.txt" % (fb_raw[w], t, topo_to_gbps[t], topo_to_dc[t], w, t)

ntt_to_gbps = dict()
ntt_to_gbps["ntt_new"] = 3786
ntt_to_dc = dict()
ntt_to_dc["ntt_new"] = 9
for t in ntt_to_gbps:
    for w in workload_max_tasks:
        print "python src/parsers/benchmark_ntt_new_combiner.py data/raw_traces/%s %d data/gml/%s.gml %f %d data/FB_Arrival.txt > %s-%s.txt" % (workload_raw[w], workload_max_tasks[w], t, ntt_to_gbps[t], ntt_to_dc[t], w, t)

for t in ntt_to_gbps:
    for w in fb_raw:
        print "python src/parsers/fb_ntt_new_combiner.py data/raw_traces/%s data/gml/%s.gml %f %d data/FB_Arrival.txt > %s-%s.txt" % (fb_raw[w], t, ntt_to_gbps[t], ntt_to_dc[t], w, t)
