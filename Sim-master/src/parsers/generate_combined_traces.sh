#!/bin/bash

MINARGS=3
MAXARGS=4
if [ "$#" -lt "$MINARGS" ]; then
    echo "Usage: $0 <path_to_processed_benchmarks_dir> <path_to_topology_data_dir> <dst_dir> [path_to_arrival_dist]"
    exit 1
fi

processed_dir=$1
topo_dir=$2
dst_dir=$3

if [ "$#" -eq "$MAXARGS" ]; then
    arrival_dist=$4
fi

for topo in swan AttMpls gb4 Ntt; do
    for trace in BigBench FB TPC-DS TPC-H; do
        trace_file=$processed_dir/${trace}.txt
        if [ "$topo" == "swan" ]; then
            trace_file=$processed_dir/$trace-5.txt
        fi

        topo_file=$topo_dir/${topo}.gml
        out_file=$dst_dir/${trace}-${topo}.txt

        if [ "$#" -eq "$MAXARGS" ]; then
            python benchmark_topology_combiner.py $trace_file $topo_file $arrival_dist > $out_file
        else
            python benchmark_topology_combiner.py $trace_file $topo_file > $out_file
        fi
    done
done
