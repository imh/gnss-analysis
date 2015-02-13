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


from gnss_analysis.abstract_analysis.manage_tests import SITL
from gnss_analysis.data_io import load_sdiffs_and_pos
import gnss_analysis.utils as ut
import swiftnav.dgnss_management as mgmt
import numpy as np
from gnss_analysis.tests.count import CountR
from gnss_analysis.tests.iar_bools import *


def determine_static_ecef(ecef_df):
  """
  Determine the static position of a receiver from a time series of it's
  position in ECEF.
  TODO: Empirically, the mean works better than median, but we don't want to be
  succeptible to outliers, so we should eventually switch to a truncated mean.
  I would first try switching to LLH/local NED, take coordinate-wise medians,
  then cut off the x percent of observation furthest from this position, and
  take the mean of the result in some rectangular frame (ECEF/NED).

  Parameters
  ----------
  ecef_df : DataFrame
    A time series of ECEF positions.

  Returns
  -------
  array
    An estimate of the receiver's position.
  """
  return np.array(ecef_df.mean(axis=0))

def guess_single_point_baselines(local_ecef_df, remote_ecef_df):
  """
  Use single point positions to estimate the baseline.

  Parameters
  ----------
  local_ecef_df : DataFrame
    A time series of single point ECEF positions of the local receiver.
  remote_ecef_df : DataFrame
    A time series of single point ECEF positions of the remote receiver.

  Returns
  -------
  array
    An estimate of the baseline (local minus remote).
  """
  loc = determine_static_ecef(local_ecef_df)
  rem = determine_static_ecef(remote_ecef_df)
  return loc - rem



class DGNSSUpdater(object):
  """
  Wraps an update function to be used by the SITL analysis.

  Parameters
  ----------
  first_data_point : DataFrame
    The first set of observations for initializing the filters.
  local_ecef_df : DataFrame
    A time series of single point ECEF positions of the local receiver.
  remote_ecef_df : DataFrame
    A time series of single point ECEF positions of the remote receiver.
  """
  def __init__(self, first_data_point, local_ecef):
    mgmt.dgnss_init(first_data_point.apply(ut.mk_swiftnav_sdiff, axis=0).dropna(), local_ecef)
  def update_function(self, datum, parameters):
    """
    An state update function to be called by the SITL analyzer.

    Parameters
    ----------
    datum : DataFrame
      A DataFrame of data necessary to create a set of sdiff_t.
    """
    mgmt.dgnss_update(datum.apply(ut.mk_swiftnav_sdiff, axis=0).dropna(), parameters.local_ecef)

class DGNSSParameters(object):
  """
  Holds parameters used during state updating and analysis.
  """
  def __init__(self, known_baseline, local_ecef_df, remote_ecef_df):
    self.local_ecef = determine_static_ecef(local_ecef_df)
    self.single_point_baseline = \
      guess_single_point_baselines(local_ecef_df, remote_ecef_df)
    self.known_baseline = known_baseline

def main():
  """
  Main entry point for running DGNSS SITL analysis.
  """
  import argparse
  parser = argparse.ArgumentParser(description='RTK Filter SITL tests.')
  parser.add_argument('file', help='Specify the HDF5 file to use.')
  args = parser.parse_args()
  hdf5_file = args.file

  data, local_ecef_df, remote_ecef_df = load_sdiffs_and_pos(hdf5_file)
  if len(data.items) < 2:
    raise Exception("Data must contain at least two observations.")
  first_datum = data.ix[0]
  data = data.ix[1:]

  known_baseline = np.array([0,0,0])
  parameters = DGNSSParameters(known_baseline, local_ecef_df, remote_ecef_df)
  updater = DGNSSUpdater(first_datum, parameters.local_ecef)

  tester = SITL(updater.update_function, data, parameters)
  tester.add_report(CountR())
  tester.add_report(FixedIARBegunR())
  tester.add_report(FixedIARCompletedR())
  tester.add_report(FixedIARLeastSquareStartedInPoolR())
  tester.add_report(FixedIARLeastSquareEndedInPoolR())

  reports = tester.compute()
  for key, report in reports.iteritems():
    print '(key=' + key + ') \t' + str(report)


if __name__ == "__main__":
  main()
