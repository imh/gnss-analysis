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
from gnss_analysis.tests.kf_internals import *
from gnss_analysis.tests.outputs import *
import swiftnav.coord_system as cs

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
  def __init__(self, known_baseline, local_ecef_df, remote_ecef_df, baseline_is_NED):
    self.local_ecef = determine_static_ecef(local_ecef_df)
    self.single_point_baseline = \
      guess_single_point_baselines(local_ecef_df, remote_ecef_df)
    if baseline_is_NED:
      self.known_baseline = cs.wgsned2ecef(known_baseline, self.local_ecef)
    else:
      self.known_baseline = known_baseline

reports = [ CountR()
          , FixedIARBegunR()
          , FixedIARCompletedR()
          , FixedIARLeastSquareStartedInPoolR()
          , FixedIARLeastSquareEndedInPoolR()
          # , FloatBaselineR()
          # , FixedBaselineR()
          # , KFSatsR()
          # , KFMeanR()
          # , KFCovR()
          ]

def run(hdf5_filename, known_baseline, reports=reports, baseline_is_NED=False):
  """
  ALternative entry point for running DGNSS SITL analysis.
  """
  data, local_ecef_df, remote_ecef_df = load_sdiffs_and_pos(hdf5_filename)
  if len(data.items) < 2:
    raise Exception("Data must contain at least two observations.")
  # data = data.ix[:,:,[0,2,22,30,31]]
  first_datum = data.ix[1]
  data = data.ix[2:]

  parameters = DGNSSParameters(known_baseline, local_ecef_df, remote_ecef_df, baseline_is_NED)
  print parameters.known_baseline

  updater = DGNSSUpdater(first_datum, parameters.local_ecef)

  initial_sats = mgmt.get_sats_management()[1]
  initial_means = mgmt.get_amb_kf_mean()

  tester = SITL(updater.update_function, data, parameters)
  tester.add_reports(reports)

  return tester.compute()

def main():
  """
  Main entry point for running DGNSS SITL analysis.
  """
  import argparse
  parser = argparse.ArgumentParser(description='RTK Filter SITL tests.')
  parser.add_argument('file', help='Specify the HDF5 file to use.')
  parser.add_argument('baselineX', help='The baseline north component.')
  parser.add_argument('baselineY', help='The baseline east  component.')
  parser.add_argument('baselineZ', help='The baseline down component.')
  parser.add_argument('--NED', action='store_true')
  args = parser.parse_args()
  hdf5_filename = args.file
  baselineX = args.baselineX
  baselineY = args.baselineY
  baselineZ = args.baselineZ
  args.NED

  baseline = np.array(map(float,[baselineX, baselineY, baselineZ]))

  reports = run(hdf5_filename, baseline, baseline_is_NED=args.NED)
  for key, report in reports.iteritems():
    print '(key=' + key + ') \t' + str(report)




if __name__ == "__main__":
  main()
