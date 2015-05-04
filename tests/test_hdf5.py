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

from sbp.client.loggers.json_logger import JSONLogIterator
from gnss_analysis.hdf5 import StoreToHDF5
from numpy import nan
from pandas.tslib import Timestamp
import os
import pandas as pd
import pytest


def test_hdf5():
  log_datafile \
    = "./data/serial_link_log_20150314-190228_dl_sat_fail_test1.log.json.dat"
  filename = log_datafile + ".hdf5"
  processor = StoreToHDF5()
  with JSONLogIterator(log_datafile) as log:
    for delta, timestamp, msg in log.next():
      processor.process_message(msg)
    processor.save(filename)
  assert os.path.isfile(filename)
  with pd.HDFStore(filename) as store:
    assert store
    assert isinstance(store.base_obs, pd.Panel)
    assert store.base_obs.shape == (91, 4, 5)
    assert store.base_obs[0, :, :].to_dict()\
      == {10: {'L': 10374.46875,
               'P': 22165269.800000001,
               'cn0': 22.0,
               'lock': 3381.0},
          12: {'L': 64486.11328125,
               'P': 20302353.989999998,
               'cn0': 46.0,
               'lock': 63239.0},
          14: {'L': 120079.03125,
               'P': 22980000.0,
               'cn0': 18.0,
               'lock': 64615.0},
          16: {'L': 99532.98828125,
               'P': 20753378.68,
               'cn0': 48.0,
               'lock': 31377.0},
          27: {'L': -5162.3359375,
               'P': 19956188.879999999,
               'cn0': 18.0,
               'lock': 1482.0}}
    assert isinstance(store.ephemerides, pd.Panel)
    assert store.ephemerides.shape == (2, 26, 3)
    assert isinstance(store.rover_obs, pd.Panel)
    assert store.rover_obs.shape == (3, 4, 8)
    assert store.rover_obs[0, :, :].to_dict() \
      == {0: {'L': 49613.0,
              'P': 22980000.0,
              'cn0': 14.0,
              'lock': 10583.0},
          6: {'L': -154459.46484375,
              'P': 21443899.43,
              'cn0': 29.0,
              'lock': 24867.0},
          10: {'L': -4688.3203125,
               'P': 22089093.379999999,
               'cn0': 14.0,
               'lock': 24238.0},
          12: {'L': 82371.0859375,
               'P': 20239084.489999998,
               'cn0': 41.0,
               'lock': 55100.0},
          14: {'L': 161655.17578125,
               'P': 22931626.530000001,
               'cn0': 21.0,
               'lock': 6171.0},
          16: {'L': 165409.55078125,
               'P': 20705699.739999998,
               'cn0': 37.0,
               'lock': 60164.0},
          27: {'L': -33727.6875,
               'P': 19874306.629999999,
               'cn0': 39.0,
               'lock': 21822.0},
          29: {'L': -80021.1796875,
               'P': 19864244.440000001,
               'cn0': 50.0,
               'lock': 42862.0}}
    assert isinstance(store.rover_rtk, pd.DataFrame)
    assert store.rover_rtk.shape == (0, 0)
    assert isinstance(store.rover_spp, pd.DataFrame)
    assert store.rover_spp.shape == (7, 5)
    assert store.rover_spp[2:5].to_dict() \
      == {Timestamp('2015-03-15 02:02:23.600000'):
          {'n_sats': 8.0,
           'tow': 7343.6000000000004,
           'x': -2704372.4505344247},
          Timestamp('2015-03-15 02:02:23.700000'):
          {'n_sats': 8.0,
           'tow': 7343.6999999999998,
           'x': -2704372.2245847895},
          Timestamp('2015-03-15 02:02:23.800000'):
          {'n_sats': 8.0,
           'tow': 7343.8000000000002,
           'x': -2704372.5723111397},
          Timestamp('2015-03-15 02:02:23.900000'):
          {'n_sats': 8.0,
           'tow': 7343.8999999999996,
           'x': -2704371.0836650538},
          Timestamp('2015-03-15 02:02:24'):
          {'n_sats': 8.0,
           'tow': 7344.0,
           'x': -2704370.9813769697}}


@pytest.mark.skipif(True, reason="Add approx. equality test later.")
def test_ephemeris_log():
  """Test ephemeris data output by hdf5 tool. Will currently fail
  because we're not checking for approx. precision test.

  """
  log_datafile \
    = "./data/serial_link_log_20150314-190228_dl_sat_fail_test1.log.json.dat"
  filename = log_datafile + ".hdf5"
  processor = StoreToHDF5()
  with JSONLogIterator(log_datafile) as log:
    for delta, timestamp, msg in log.next():
      processor.process_message(msg)
    processor.save(filename)
  assert os.path.isfile(filename)
  with pd.HDFStore(filename) as store:
    assert store.ephemerides[:, :, 27].to_dict() \
      == {Timestamp('2015-03-15 03:59:44'):
          {'c_rs': nan, 'toe_wn': nan, 'prn': nan,
           'inc_dot': nan, 'tgd': nan, 'c_rc': nan, 'toc_wn': nan,
           'sqrta': nan, 'omegadot': nan, 'inc': nan, 'toe_tow': nan,
           'c_uc': nan, 'c_us': nan, 'valid': nan, 'm0': nan,
           'toc_tow': nan, 'dn': nan, 'ecc': nan, 'c_ic': nan,
           'c_is': nan, 'healthy': nan, 'af1': nan, 'w': nan,
           'af0': nan, 'omega0': nan, 'af2': nan},
          Timestamp('2015-03-15 04:00:00'):
          {'c_rs': 15.96875, 'toe_wn': 1836.0, 'prn': 27.0,
           'inc_dot': 2.7322566666000417e-10,
           'tgd': -1.1175870895385742e-08, 'c_rc': 320.96875,
           'toc_wn': 1836.0, 'sqrta': 5153.6934394836426,
           'omegadot': -7.7553230403337661e-09,
           'inc': 0.98869366123094204, 'toe_tow': 14400.0,
           'c_uc': 9.2200934886932373e-07, 'c_us': 3.468245267868042e-06,
           'valid': 1.0, 'm0': -2.3437882587715801, 'toc_tow': 14400.0,
           'dn': 4.0358823964157481e-09, 'ecc': 0.019611002877354622,
           'c_ic': 2.4586915969848633e-07, 'c_is': 1.4528632164001465e-07,
           'healthy': 1.0, 'af1': 2.6147972675971687e-12,
           'w': -1.6667971409741453, 'af0': 0.00042601628229022026,
           'omega0': -2.7040169769321869, 'af2': 0.0}}
