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

"""Generates an Pandas-compatible, HDF5 file of GPS observations,
ephemerides, and estimated baseline and single point solutions from a
HITL testing log. Indexed by GPS time with observable quantities, like
pseudorange and carrier phase, converted to SI units.

"""

from sbp.client.loggers.json_logger import JSONLogIterator
from sbp.utils import exclude_fields
import os
import pandas as pd
import sbp.navigation as nav
import sbp.observation as ob
import sbp.tracking as tr
import swiftnav.gpstime as gpstime

# Units conversations (length, time, and 8-bit fractional cycles)
MAX_SATS = 32
MSEC_TO_SECONDS = 1000.
MM_TO_M = 1000.
CM_TO_M = 100.
Q32_WIDTH = 256.
from_base = lambda msg: msg.sender == 0
time_fn = gpstime.gpst_components2datetime


class StoreToHDF5(object):
  """Stores observations as HDF5.

  """

  def __init__(self):
    self.base_obs = {}
    self.rover_obs = {}
    self.ephemerides = {}
    self.rover_spp = {}
    self.rover_rtk_ned = {}
    self.rover_rtk_ecef = {}
    self.time = None

  def _process_obs(self, msg):
    if type(msg) is ob.MsgObs:
      time = time_fn(msg.header.t.wn, msg.header.t.tow / MSEC_TO_SECONDS)
      t = self.base_obs if from_base(msg) else self.rover_obs
      # Convert pseudorange, carrier phase to SI units.
      for o in msg.obs:
        v = {'P': o.P / CM_TO_M, 'L': o.L.i + o.L.f / Q32_WIDTH,
             'cn0': o.cn0, 'lock': o.lock}
        if time in t:
          t[time].update({o.prn: v})
        else:
          t[time] = {o.prn: v}

  def _process_eph(self, msg):
    if type(msg) is ob.MsgEphemeris or type(msg) is tr.MsgEphemerisOld:
      if msg.healthy == 1 and msg.valid == 1:
        time = gpstime.gpst_components2datetime(msg.toe_wn, msg.toe_tow)
        m = exclude_fields(msg)
        if time in self.ephemerides:
          self.ephemerides[time].update({msg.prn: m})
        else:
          self.ephemerides[time] = {msg.prn: m}

  def _process_pos(self, msg):
    if type(msg) is nav.MsgGPSTime:
      self.time = msg
    elif self.time is not None:
      m = exclude_fields(msg)
      if type(msg) is nav.MsgPosECEF:
        time = time_fn(self.time.wn, msg.tow / MSEC_TO_SECONDS)
        m['tow'] /= MSEC_TO_SECONDS
        self.rover_spp[time] = m
      elif type(msg) is nav.MsgBaselineNED:
        time = time_fn(self.time.wn, msg.tow / MSEC_TO_SECONDS)
        m['tow'] /= MSEC_TO_SECONDS
        m['n'] /= MM_TO_M
        m['e'] /= MM_TO_M
        m['d'] /= MM_TO_M
        self.rover_rtk_ned[time] = m
      elif type(msg) is nav.MsgBaselineECEF:
        time = time_fn(self.time.wn, msg.tow / MSEC_TO_SECONDS)
        m['tow'] /= MSEC_TO_SECONDS
        m['x'] /= MM_TO_M
        m['y'] /= MM_TO_M
        m['z'] /= MM_TO_M
        self.rover_rtk_ecef[time] = m

  def process_message(self, msg):
    self._process_pos(msg)
    self._process_eph(msg)
    self._process_obs(msg)

  def save(self, filename):
    if os.path.exists(filename):
      print "Unlinking %s, which already exists!" % filename
      os.unlink(filename)
    f = pd.HDFStore(filename, mode='w')
    f.put('base_obs', pd.Panel(self.base_obs))
    f.put('rover_obs', pd.Panel(self.rover_obs))
    f.put('ephemerides', pd.Panel(self.ephemerides))
    f.put('rover_spp', pd.DataFrame(self.rover_spp))
    f.put('rover_rtk_ned', pd.DataFrame(self.rover_rtk_ned))
    f.put('rover_rtk_ecef', pd.DataFrame(self.rover_rtk_ecef))
    f.close()


def main():
  """Fuck some Pandas

  """
  import argparse
  import time
  parser = argparse.ArgumentParser(description='Swift Nav SBP log parser.')
  parser.add_argument('file',
                      help='Specify the log file to use.')
  parser.add_argument('-o', '--output',
                      nargs=1,
                      help='Test results output filename.')
  parser.add_argument('-n', '--num_records',
                      nargs=1,
                      default=[None],
                      help='Number or SBP records to process.')
  args = parser.parse_args()
  filename = args.output[0]
  log_datafile = args.file
  num_records = args.num_records[0]
  processor = StoreToHDF5()
  i = 0
  logging_interval = 10000
  start = time.time()
  with JSONLogIterator(log_datafile) as log:
    for delta, timestamp, msg in log.next():
      i += 1
      if i % logging_interval == 0:
        print "Processed %d records! @ %s sec." \
          % (i, time.time() - start)
      processor.process_message(msg)
      if num_records is not None and i >= int(num_records):
        print "Processed %d records!" % i
        break
    processor.save(filename)

if __name__ == "__main__":
  main()
