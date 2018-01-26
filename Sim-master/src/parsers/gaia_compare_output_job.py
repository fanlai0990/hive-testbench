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
    if len(sys.argv) >= 3:
        csv_filename1 = sys.argv[1]
        csv_filename2 = sys.argv[2]
    else:
        print "Usage: " + sys.argv[0] + " <baseline_csv_filename1> <coflow_csv_filename2>"
        sys.exit(1)

    def get_dict(csv_filename):
        df = pd.read_csv(csv_filename)
        ids = df.JobID.values.tolist()
        cmp = df.JobCompletionTime.values.tolist()
        return dict(zip(ids, cmp))

    dic1 = get_dict(csv_filename1)
    dic2 = get_dict(csv_filename2)

    jobs1 = set(dic1.keys())
    jobs2 = set(dic2.keys())

    intersected = jobs1.intersection(jobs2)

    vals1 = [dic1[v] for v in intersected]
    vals2 = [dic2[v] for v in intersected]

    cdf1 = cdf_from_csv(vals1)
    cdf2 = cdf_from_csv(vals2)

    # print "-".join(csv_filename1.split("/")[-3:-1]), "-".join(csv_filename2.split("/")[-3:-1]), "CDF"
    # for x, y, z in zip(cdf1, cdf2, np.linspace(0, 1, 101)):
    #     print "%.2f %.2f %.2f" % (x, y, z)
    # print ""

    avg1 = np.average(vals1)
    avg2 = np.average(vals2)

    def my_div(v1, v2):
        if v2 == 0:
            return 0
        return v1 / v2

    sys.stdout.write("Number of jobs = %d\n" % len(intersected))
    sys.stdout.write("Avg. %7.2f %7.2f %7.2f\n" % (avg1, avg2, avg1/avg2))
    sys.stdout.write("25th %7.2f %7.2f %7.2f\n" % (cdf1[25], cdf2[25], my_div(cdf1[25], cdf2[25])))
    sys.stdout.write("50th %7.2f %7.2f %7.2f\n" % (cdf1[50], cdf2[50], my_div(cdf1[50], cdf2[50])))
    sys.stdout.write("75th %7.2f %7.2f %7.2f\n" % (cdf1[75], cdf2[75], my_div(cdf1[75], cdf2[75])))
    sys.stdout.write("95th %7.2f %7.2f %7.2f\n" % (cdf1[95], cdf2[95], my_div(cdf1[95], cdf2[95])))


if __name__ == "__main__":
    main()
