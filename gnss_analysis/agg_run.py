#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Ian Horn <ian@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

from gnss_analysis.runner import run as single_run
import pandas as pd
import numpy as np


def main():
  import argparse
  parser = argparse.ArgumentParser(description='RTK Filter SITL tests.')
  parser.add_argument('infile', help='Specify the HDF5 file to use for input.')
  parser.add_argument('outfile', help='Specify the HDF5 file to output into.')
  parser.add_argument('baselineX', help='The baseline north component.')
  parser.add_argument('baselineY', help='The baseline east  component.')
  parser.add_argument('baselineZ', help='The baseline down component.')
  parser.add_argument('--NED', action='store_true')
  parser.add_argument('-k', '--key',
                      default='table', nargs=1,
                      help='The key for the output table to insert into.')
  parser.add_argument('-r', '--row',
                      default=None, nargs=1,
                      help='The key for the output table to insert into.')

  args = parser.parse_args()
  hdf5_filename_in = args.infile
  hdf5_filename_out = args.outfile
  baselineX = args.baselineX
  baselineY = args.baselineY
  baselineZ = args.baselineZ
  baseline = np.array(map(float, [baselineX, baselineY, baselineZ]))
  out_key = args.key
  row = args.row
  if row is None:
    row = hdf5_filename_in

  reports = single_run(hdf5_filename_in, baseline, baseline_is_NED=args.NED)

  out_store = pd.HDFStore(hdf5_filename_out)
  if ('/' + out_key) in out_store.keys():
    out_df = out_store[out_key]
  else:
    out_df = pd.DataFrame()

  new_cols = [col for col in reports.keys() if col not in out_df.columns]
  for new_col in new_cols:
    out_df[new_col] = pd.Series(np.nan * np.empty_like(out_df.index),
                                index=out_df.index)
  out_df.loc[row] = pd.Series(reports)

  out_store[out_key] = out_df
  out_store.close()

if __name__ == "__main__":
  main()
