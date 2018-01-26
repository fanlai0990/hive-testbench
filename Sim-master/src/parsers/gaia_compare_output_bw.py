#!/usr/bin/env python

import itertools
import numpy as np
import math
import os
import pandas as pd
import sys

import warnings
warnings.filterwarnings("ignore")


def main():
    if len(sys.argv) >= 3:
        csv_filename1 = sys.argv[1]
        csv_filename2 = sys.argv[2]
    else:
        print "Usage: " + sys.argv[0] + " <baseline_csv_filename1> <coflow_csv_filename2>"
        sys.exit(1)

    def get_bw(csv_filename):
        df = pd.read_csv(csv_filename)
        bw = df.Bandwidth.values.tolist()
        try:
            ts = sorted(df.EndTime.values.tolist())
        except AttributeError as e:
            ts = sorted(df.TimeStamp.values.tolist())

        last = 0
        ret_val = 0.0
        for b, t in zip(bw, ts):
            ret_val += b * (t - last)
            last = t
        return ret_val / t, max(bw)

    avg_util1, max_bw1 = get_bw(csv_filename1)
    avg_util2, max_bw2 = get_bw(csv_filename2)

    print "%.2f %.2f" % (avg_util2 / avg_util1, max_bw2 / max_bw1)


if __name__ == "__main__":
    main()
