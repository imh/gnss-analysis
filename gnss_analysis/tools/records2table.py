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
ephemerides, and reported baseline and single point solutions from a
HITL testing log. These quantities are indexed by GPS time with
observable quantities, like pseudorange and carrier phase, converted
to SI units. Hopefully you should only need to do this once.

The output of this process is a Pandas Panel that looks a bit like the
following:
<class 'pandas.io.pytables.HDFStore'>
File path: data/serial-link-20150506-175750.log.json.new_fields.hdf5
/base_obs                  wide         (shape->[7272,4,8])
/ephemerides               wide         (shape->[2,26,8])
/rover_obs                 wide         (shape->[7241,4,8])
/rover_rtk_ecef            frame        (shape->[7,14545])
/rover_rtk_ned             frame        (shape->[8,14545])
/rover_spp                 frame        (shape->[7237,3])

"""

from gnss_analysis.constants import *
from sbp.client.loggers.json_logger import JSONLogIterator
from sbp.utils import exclude_fields, walk_json_dict
import os
import pandas as pd
import sbp.navigation as nav
import sbp.observation as ob
import sbp.piksi as piksi
import sbp.tracking as tr
import swiftnav.gpstime as gpstime

from_base = lambda msg: msg.sender == 0
time_fn = gpstime.gpst_components2datetime

# TODO (Buro): The Pandas HDF5 stuff can handle hierarchical keys,
# which we should probably consider using throughout this project.


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
    self.rover_tracking = {}
    self.rover_iar_state = {}
    self.time = None

  def _process_obs(self, host_offset, host_time, msg):
    if type(msg) is ob.MsgObs:
      time = time_fn(msg.header.t.wn, msg.header.t.tow / MSEC_TO_SECONDS)
      t = self.base_obs if from_base(msg) else self.rover_obs
      # Convert pseudorange, carrier phase to SI units.
      for o in msg.obs:
        v = {'P': o.P / CM_TO_M, 'L': o.L.i + o.L.f / Q32_WIDTH,
             'cn0': o.cn0, 'lock': o.lock}
        v.update({'host_offset': host_offset, 'host_time': host_time})
        if time in t:
          t[time].update({o.prn: v})
        else:
          t[time] = {o.prn: v}

  def _process_eph(self, host_offset, host_time, msg):
    if type(msg) is ob.MsgEphemeris or type(msg) is tr.MsgEphemerisOld:
      if msg.healthy == 1 and msg.valid == 1:
        time = gpstime.gpst_components2datetime(msg.toe_wn, msg.toe_tow)
        m = exclude_fields(msg)
        m.update({'host_offset': host_offset, 'host_time': host_time})
        if time in self.ephemerides:
          self.ephemerides[time].update({msg.prn: m})
        else:
          self.ephemerides[time] = {msg.prn: m}

  def _process_pos(self, host_offset, host_time, msg):
    if type(msg) is nav.MsgGPSTime:
      self.time = msg
    elif self.time is not None:
      m = exclude_fields(msg)
      m.update({'host_offset': host_offset, 'host_time': host_time})
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

  def _process_tracking(self, host_offset, host_time, msg):
    if type(msg) is tr.MsgTrackingState:
      m = exclude_fields(msg)
      # Flatten a bit: reindex at the top level by prn and remove the
      # 'states' field from the message.
      for s in msg.states:
        m[s.prn] = walk_json_dict(s)
        m[s.prn].update({'host_offset': host_offset,
                         'host_time': host_time})
      self.rover_tracking[host_offset] = m
      del m['states']

  def _process_iar(self, host_offset, host_time, msg):
    if type(msg) is piksi.MsgIarState:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      self.rover_iar_state[host_offset] = m

  def process_message(self, host_offset, host_time, msg):
    self._process_pos(host_offset, host_time, msg)
    self._process_eph(host_offset, host_time, msg)
    self._process_obs(host_offset, host_time, msg)
    self._process_tracking(host_offset, host_time, msg)
    self._process_iar(host_offset, host_time, msg)

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
    f.put('rover_tracking', pd.Panel(self.rover_tracking))
    f.put('rover_iar_state', pd.DataFrame(self.rover_iar_state))
    f.close()


def main():
  """Fuck some Pandas

  """
  import argparse
  import time
  parser = argparse.ArgumentParser(description='Swift Nav SBP log to HDF5 table tool.')
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
  log_datafile = args.file
  if args.output is None:
    filename = log_datafile + '.hdf5'
  else:
    filename = args.output[0]
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
      processor.process_message(delta, timestamp, msg)
      if num_records is not None and i >= int(num_records):
        print "Processed %d records!" % i
        break
    processor.save(filename)

if __name__ == "__main__":
  main()
