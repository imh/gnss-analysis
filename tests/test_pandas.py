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

"""Basic integration tests for pandas GPS time interpolation
utilities.

"""

from pandas.tslib import Timestamp, Timedelta
import gnss_analysis.tables as t
import numpy as np
import os
import pandas as pd


def test_interpolate_gps_time():
  filename = "data/serial-link-20150429-163230.log.json.hdf5"
  assert os.path.isfile(filename)
  with pd.HDFStore(filename) as store:
    idx = store.rover_spp.T.host_offset.reset_index()
    model = t.interpolate_gpst_model(idx)
    assert isinstance(model, pd.stats.ols.OLS)
    assert np.allclose([model.beta.x, model.beta.intercept],
                       [1.00000368376, -64.2579561376])
    init_offset = store.rover_spp.T.host_offset[0]
    init_date = store.rover_spp.T.index[0]
    f = lambda t1: t.apply_gps_time(t1*t.MSEC_TO_SEC, init_date, model)
    dates = store.rover_logs.T.host_offset.apply(f)
    l = dates.tolist()
    start, end = l[0], l[-1]
    assert start == Timestamp("2015-04-29 23:32:55.272075")
    assert end == Timestamp("2015-04-29 23:57:46.457568")
    init_secs_offset \
      = store.rover_spp.T.host_offset[0] - store.rover_logs.T.index[0]
    assert np.allclose([init_secs_offset*t.MSEC_TO_SEC], [55.859])
    assert (init_date - start) == Timedelta('0 days 00:00:55.848925')
    assert (end - init_date) == Timedelta('0 days 00:23:55.336568')
    assert pd.DatetimeIndex(dates).is_monotonic_increasing
    assert dates.shape == (2457,)

def test_gps_time_col():
  filename = "data/serial-link-20150429-163230.log.json.hdf5"
  assert os.path.isfile(filename)
  with pd.HDFStore(filename) as store:
    tables = ['rover_iar_state', 'rover_logs', 'rover_tracking']
    t.get_gps_time_col(store, tables)
    gpst = store.rover_iar_state.T.approx_gps_time
    assert gpst.shape == (1487,)
    assert pd.DatetimeIndex(gpst).is_monotonic_increasing
    gpst = store.rover_logs.T.approx_gps_time
    assert gpst.shape == (2457,)
    assert pd.DatetimeIndex(gpst).is_monotonic_increasing
    gpst = store.rover_tracking[:, 'approx_gps_time', :]
    assert gpst.shape == (32, 7248)
