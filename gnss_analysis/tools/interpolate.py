#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Ian Horn <ian@swiftnav.com>
#          Bhaskar Mookerji <mookerji@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

"""The interpolate tool takes generated HDF5 output from records2table
and fills in derived or interpolated observation quantities.

For example, passing an HDF5 store that looks like
<class 'pandas.io.pytables.HDFStore'>
File path: /home/mookerji/projects/swift/gnss-analysis/test2.hdf5
/base_obs                   wide         (shape->[6948,6,9])
/ephemerides                wide         (shape->[1,28,10])
/rover_iar_state            frame        (shape->[3,1487])
/rover_logs                 frame        (shape->[1,3907])
/rover_obs                  wide         (shape->[6954,6,10])
/rover_rtk_ecef             frame        (shape->[9,14112])
/rover_rtk_ned              frame        (shape->[10,14112])
/rover_spp                  frame        (shape->[9,14116])
/rover_tracking             wide         (shape->[7370,5,32])

will decorate the rover_iar_state, rover_logs, and rover_tracking
messages with their interpolated GPS time. Each will have a new column
titled 'approx_gps_time'.

"""

from gnss_analysis.tables import get_gps_time_col, reindex_tables
import pandas as pd

def main():
  import argparse
  import sys
  parser = argparse.ArgumentParser(description='Swift Nav derive/interpolate obs.')
  parser.add_argument('file',
                      help='Specify the log file to use.')
  parser.add_argument('-n', '--num_records',
                      nargs=1,
                      default=[None],
                      help='Number of GPS observation records to process.')
  parser.add_argument('-v', '--verbose',
                      action='store_true',
                      help='Verbose output.')
  gps_time_tabs = ['rover_iar_state', 'rover_logs', 'rover_tracking',
                   'rover_acq', 'rover_thread_state', 'rover_uart_state']
  # TODO (Buro): Add in handling for explicit overwrites. Currently,
  # this will fill in and overwrite (specifically sdiffs, etc.) that
  # you might have.
  #
  # parser.add_argument('-o', '--output',
  #                     nargs=1,
  #                     default=[None],
  #                     help='Test results output filename')
  # parser.add_argument('-w', '--overwrite',
  #                     action='store_true'
  #                     help='Overwrite .')
  args = parser.parse_args()
  log_datafile = args.file
  num_records = args.num_records[0]
  verbose = args.verbose
  with pd.HDFStore(log_datafile) as store:
    try:
      if verbose:
        print "Verbose output specified..."
        print "Loading table %s ." % str(store)
        print "Interpolating times for tables %s." % ', '.join(gps_time_tabs)
      if not store.rover_spp.empty:
        get_gps_time_col(store, gps_time_tabs, verbose=verbose)
        reindex_tables(store,
                       ['rover_iar_state', 'rover_logs'],
                       verbose=verbose)
      else:
        raise Exception("No single-point solutions available for interpolation.")
    except (KeyboardInterrupt, SystemExit):
      print "Exiting!"
      sys.exit()
    finally:
      store.close()

if __name__ == "__main__":
  main()
