#!/usr/bin/env python

import itertools
import numpy as np
import math
import os
import pandas as pd
import sys

import warnings
warnings.filterwarnings("ignore")


def cdf_from_csv(vals):
    vals = sorted(vals)

    ret_val = list()

    delta = 0.01
    row_id = 1
    i = 1
    p = delta
    num_rows = len(vals)
    for v in vals:
        if row_id == 1:
            ret_val.append(v)
        elif row_id == num_rows:
            ret_val.append(v)
        else:
            if float(i) / num_rows >= p or (abs(float(i) / num_rows - p) < 1e-6):
                ret_val.append(v)
                p += delta
            i += 1
        row_id += 1

    return ret_val


def main():
    if len(sys.argv) >= 4:
        csv_filename1 = sys.argv[1]
        csv_filename2 = sys.argv[2]
        size_filename = sys.argv[3]
    else:
        print "Usage: " + sys.argv[0] + " <baseline_csv_filename1> <coflow_csv_filename2> <size_dict>"
        sys.exit(1)

    def get_dict(csv_filename):
        df = pd.read_csv(csv_filename)
        ids = df.JobID.values.tolist()
        cmp = df.JobCompletionTime.values.tolist()
        return dict(zip(ids, cmp))

    dic1 = get_dict(csv_filename1)
    dic2 = get_dict(csv_filename2)

    with open(size_filename, "r") as f:
        size_dict = dict([(int(l.split()[0]), float(l.split()[1])) for l in f.readlines()])

    ratios = list()
    sizes = list()
    count = 0
    minimum = 1.0
    for j1 in dic1:
        if j1 in dic2 and j1 in size_dict and dic1[j1] != 0 and dic2[j1] != 0:
            ratio = dic1[j1] / dic2[j1]
            ratios.append(ratio)
            if ratio < 1:
                count += 1
            if ratio < minimum:
                minimum = ratio
            sizes.append(size_dict[j1])

    print np.corrcoef(ratios, sizes)
    print 100.0 * count / len(ratios), "% jobs became worse by at most", minimum


if __name__ == "__main__":
    main()
