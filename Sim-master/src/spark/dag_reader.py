import random
from . import Stage, JobDAG


class Reader(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.job_dict = dict()


class TraceReader(Reader):
    def __init__(self, file_path, net_graph):
        super(TraceReader, self).__init__(file_path)
        self.net_graph = net_graph

        # Use a fixed seed for random number generation purposes to determine placements
        self.my_random = random.Random(13)

    def create_job_dict(self):
        fo = open(self.file_path, "rw+")
        lines = fo.readlines()
        fo.close()

        lid = 0
        num_jobs = 0
        while lid < len(lines) and num_jobs < 1000:
            if lines[lid].startswith("#"):
                lid += 1

            splits = lines[lid].split()
            lid += 1
            num_stages = int(splits[0])
            job_id = splits[1]
            arrival_time = int(splits[2])

            current_stage_dict = {}
            for x in range(num_stages):
                splits = lines[x + lid].split()
                stage_id = splits[0]
                num_tasks = int(float(splits[1]))

                # Use already assigned task locations;
                # Otherwise, assign each of the num_tasks tasks of this stage to random network nodes
                if len(splits) > 2:
                    task_locs = splits[2:num_tasks+2]
                else:
                    net_nodes = self.net_graph.nodes.keys()
                    self.my_random.shuffle(net_nodes)
                    task_locs = net_nodes[:num_tasks]

                if stage_id.startswith("M"):
                    stage_type = "MAP"
                else:
                    stage_type = "REDUCE"
                current_stage_dict[stage_id] = Stage(job_id, stage_id, stage_type, task_locs, self.my_random, self.net_graph)
            lid += num_stages

            splits = lines[lid].split()
            lid += 1
            num_edges = int(splits[0])

            # First off, vertex is a misnomer; these are edges. Still using them to match with prior code.
            current_vertexs = []
            for x in range(num_edges):
                splits = lines[x + lid].split()
                src_stage = splits[0]
                dst_stage = splits[1]
                data_size = max(1, long(splits[2])) * 8 # multiply 8 to convert it into mega bits
                current_vertexs.append((src_stage, dst_stage, data_size))
                # The following is enforced
                # data_size is the total bytes to send from all tasks of src_stage (M) to all tasks of dst_stage (R).
                # Meaning, each of the (M * R) flows between these two stages will transfer (data_size / (M * R)) bytes.
            lid += num_edges

            self.job_dict[job_id] = JobDAG(job_id=job_id,
                                           stage_dict=current_stage_dict,
                                           vertexs=current_vertexs,
                                           net_graph=self.net_graph,
                                           timestamp=arrival_time)
            num_jobs += 1

        return self.job_dict
