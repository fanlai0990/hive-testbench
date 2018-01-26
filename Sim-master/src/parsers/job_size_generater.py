from collections import defaultdict
import os
import sys


def main():
    if len(sys.argv) == 2:
        combined_trace_file = sys.argv[1]
    else:
        print "Usage: ", sys.argv[0], "<path_to_fb_coflow_trace>"
        sys.exit(1)

    fo = open(combined_trace_file, 'r+')
    lines = fo.readlines()
    fo.close()

    combined_trace_file = combined_trace_file.replace('.', ' ')
    combined_trace_file = combined_trace_file.replace('/', ' ')
    combined_trace_file = (combined_trace_file.split())[-2]

    output_dir = os.path.dirname(__file__) + '/../../data/job_size'
    output_file = os.path.dirname(__file__) + '/../../data/job_size/' + combined_trace_file + '-size.txt'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    fo = open(output_file, 'w+')

    lid = 0
    while lid < len(lines):
        if len(lines[lid]) == 0 or lines[lid].startswith('#'):
            job_size_sum = 0
            stage_loc_dict = defaultdict(set)

            lid += 1
            splits = lines[lid].split()
            stage_num = int(splits[0])
            job_id = splits[1]

            lid += 1
            for i in range(stage_num):
                splits = lines[lid + i].split()
                stage_name = splits[0]
                stage_loc_num = int(splits[1])
                for j in range(stage_loc_num):
                    stage_loc_dict[stage_name].add(splits[j+2])

            lid += stage_num
            num_coflow = int(lines[lid])

            lid += 1
            for i in range(num_coflow):
                splits = lines[lid + i].split()
                child_stage = splits[0]
                parent_stage = splits[1]
                volume = int(splits[2])

                trace_flow_num = 0
                actual_flow_num = 0
                for c_loc in stage_loc_dict[child_stage]:
                    for p_loc in stage_loc_dict[parent_stage]:
                        trace_flow_num += 1
                        if c_loc != p_loc:
                            actual_flow_num += 1
                job_size_sum += float(actual_flow_num) / trace_flow_num * volume

            lid += num_coflow

            fo.write(job_id + ' ' + str(job_size_sum) + '\n')

        else:
            print 'parsing error'
            break


if __name__ == "__main__":
    main()
