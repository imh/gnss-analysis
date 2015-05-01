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
import os
import pandas as pd


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
    assert isinstance(store.base, pd.Panel)
    assert store.base.shape == (91, 4, 5)
    assert isinstance(store.ephemerides, pd.Panel)
    assert store.ephemerides.shape == (2, 26, 3)
    assert isinstance(store.rover, pd.Panel)
    assert store.rover.shape == (3, 4, 8)
    assert isinstance(store.rover_rtk, pd.DataFrame)
    assert store.rover_rtk.shape == (0, 0)
    assert isinstance(store.rover_spp, pd.DataFrame)
    assert store.rover_spp.shape == (7, 5)
