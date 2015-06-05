#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Bhaskar Mookerji <mookerji@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

"""Interpolation utilities for Pandas tables.

"""

import numpy as np
import pandas as pd
import warnings

USEC_TO_SEC = 1e-6
MSEC_TO_SEC = 1e-3

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


def interpolate_gpst_model(df_gps):
  """Produces a linear mapping between the host's log offset (seconds)
  and GPS offset (seconds) from the beginning of the log. Assumes that
  the first GPS time as the initial GPS time.

  Parameters
  ----------
  host_offset : pandas.DataFrame

  Returns
  ----------
  pandas.stats.ols.OLS

  """
  init_gps_t = pd.to_datetime(df_gps['index'][0])
  gps_offset = pd.to_datetime(df_gps['index']) - init_gps_t
  gps_offset_y = gps_offset / np.timedelta64(1, 's')
  log_offset_x = df_gps.host_offset*MSEC_TO_SEC
  return pd.ols(y=gps_offset_y, x=log_offset_x, intercept=True)


def apply_gps_time(host_offset, init_date, model):
  """Interpolates a GPS datetime based on a record's host log offset.

  Parameters
  ----------
  host_offset : int
    Second offset since beginning of log.
  model : pandas.stats.ols.OLS
    Pandas OLS model mapping host offset to GPS offset

  Returns
  ----------
  pandas.tslib.Timestamp

  """
  gps_offset = model.beta.x * host_offset + model.beta.intercept
  return init_date + pd.Timedelta(seconds=gps_offset)


def get_gps_time_col(store, tabs, verbose=False):
  """Given an HDFStore and a list of tables in that HDFStore,
  interpolates GPS times for the desired tables and inserts the
  appropriate columns in the table.

  Parameters
  ----------
  store : pandas.HDFStore
    Pandas HDFStore
  tabs : list
    List of tables to interpolate for

  """
  idx = store.rover_spp.T.host_offset.reset_index()
  model = interpolate_gpst_model(idx)
  init_date = store.rover_spp.T.index[0]
  f = lambda t1: apply_gps_time(t1*MSEC_TO_SEC, init_date, model)
  gpst_key = 'approx_gps_time'
  for tab in tabs:
    # Because this is largely a research tool and the tables are
    # constantly in flux, just warn if the specified table isn't in
    # the table when interpolating.
    if verbose:
      print "Interpolating approx_gps_time for %s." % tab
    if tab not in store:
      warnings.warn("%s not found in Pandas table" % tab, UserWarning)
    elif isinstance(store[tab], pd.DataFrame):
      dft = store[tab].T
      dft[gpst_key] = store[tab].T.host_offset.apply(f)
      store[tab] = dft.T
    elif isinstance(store[tab], pd.Panel):
      y = {}
      for prn in store[tab].items:
        y[prn] = store[tab][prn, 'host_offset', :].dropna().apply(f)
      ans = store[tab].transpose(1, 0, 2)
      ans['approx_gps_time'] = pd.DataFrame(y).T
      store[tab] = ans.transpose(1, 0, 2)
