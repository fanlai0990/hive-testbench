# Multi-path-Coflow simulator

## Requirement
`Matplotlib`, `pulp` and `netgraphx` is required for running this simulator

## How to run the simulator
To view the help menu on how to use our program, run the following command:

`python simulate.py -h`

```
usage: simulate.py [-h] -g GML [-s SCHEDULER] [-c COFLOW] [-j JOB]

Simulate coflow scheduling

optional arguments:
  -h, --help            show this help message and exit
  -g GML, --gml GML     Network data in gml format
  -s SCHEDULER, --scheduler SCHEDULER
                        scheduler used. Default scheduler: recursive coflow scheduler [recursive-remain-flow]. Supported
                        scheduler: 1. recursive remain flow scheduler [recursive-remain-flow], 2. recursive coflow scheduler
                        [recursive-coflow], 3. remain flow scheduler [remain-flow], 4. reverse recursive coflow scheduler
                        [reverse-recursive-coflow], 5. combine coflow scheduler [combine-coflow], 6. baseline flow scheduler
                        use MaxMinFlow scheduler [baseline]
  -c COFLOW, --coflow COFLOW
                        path to coflow file if defined
  -j JOB, --job JOB     path to job file if defined

```

All input files are in `data/`. Network topologies are in `data/gml`, job arrival schedule files are in `data/job`

For example, to run our program with default algorithm and topology in data/gml/AttMpls.gml and job schedule in ./data/job/simpleJobs.txt:

`python simulate.py -g data/gml/AttMpls.gml -j ./data/job/simpleJobs.txt`
