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
to SI units. It will also include the UTC host time the message was
received on the serial host.

Hopefully you should only need to do this once for a given HITL JSON
file.

The output of this process is a Pandas Panel that looks a bit like the
following:
<class 'pandas.io.pytables.HDFStore'>
File path: data/serial-link-20150506-175750.log.json.fake.hdf5
/base_obs                   wide         (shape->[6948,6,9])
/ephemerides                wide         (shape->[1,28,10])
/rover_iar_state            frame        (shape->[3,1487])
/rover_logs                 frame        (shape->[1,3907])
/rover_obs                  wide         (shape->[6954,6,10])
/rover_rtk_ecef             frame        (shape->[9,14112])
/rover_rtk_ned              frame        (shape->[10,14112])
/rover_spp                  frame        (shape->[9,14116])
/rover_tracking             wide         (shape->[7370,5,32])

"""

from gnss_analysis.constants import *
from sbp.client.loggers.json_logger import JSONLogIterator
from sbp.utils import exclude_fields, walk_json_dict
import os
import pandas as pd
import sbp.acquisition as acq
import sbp.navigation as nav
import sbp.observation as ob
import sbp.deprecated as dep
import sbp.piksi as piksi
import sbp.tracking as tr
import sbp.logging as lg
import swiftnav.gpstime as gpstime
import time

from_base = lambda msg: msg.sender == 0
time_fn = gpstime.gpst_components2datetime

# TODO (Buro): The Pandas HDF5 stuff can handle hierarchical keys,
# which we should probably consider using throughout this project.

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)

class StoreToHDF5(object):
  """Stores observations as HDF5.

  """

  def __init__(self):
    self.base_obs = {}
    self.base_obs_integrity = {}
    self.rover_obs = {}
    self.rover_obs_integrity = {}
    self.ephemerides = {}
    self.rover_ephemerides = {}
    self.base_ephemerides = {}
    self.rover_spp = {}
    self.rover_llh = {}
    self.rover_rtk_ned = {}
    self.rover_rtk_ecef = {}
    self.rover_tracking = {}
    self.rover_iar_state = {}
    self.rover_logs = {}
    self.rover_thread_state = {}
    self.rover_uart_state = {}
    self.rover_acq = {}
    self.time = None

  def _process_obs(self, host_offset, host_time, msg):
    if type(msg) is ob.MsgObs:
      time = time_fn(msg.header.t.wn, msg.header.t.tow / MSEC_TO_SECONDS)
      # n_obs is split bytewise between the total and the count (which message
      # this is).
      count = 0x0F & msg.header.n_obs
      total = msg.header.n_obs >> 4
      t = self.base_obs if from_base(msg) else self.rover_obs
      ti = self.base_obs_integrity if from_base(msg) else \
           self.rover_obs_integrity
      # Convert pseudorange, carrier phase to SI units.
      for o in msg.obs:
        v = {'P': o.P / CM_TO_M, 'L': o.L.i + o.L.f / Q32_WIDTH,
             'cn0': o.cn0, 'lock': o.lock}
        v.update({'host_offset': host_offset, 'host_time': host_time})
        if time in t:
          t[time].update({o.prn: v})
        else:
          t[time] = {o.prn: v}
        # Set the 'counts' field such that the Nth bit is 1 iff we have
        # received a message whose 'count' field (the first byte of the n_obs
        # field) is N. If we have gotten them all, counts should be
        # (1 << total) - 1, and python makes the numbers really big as needed.
        if time in ti:
          ti[time].update({'counts':ti[time]['counts'] | 1 << count})
        else:
          ti[time] = {'total': total, 'counts':1 << count}

  def _process_eph(self, host_offset, host_time, msg):
    if type(msg) is ob.MsgEphemeris or type(msg) is dep.MsgEphemerisDeprecated:
      time = gpstime.gpst_components2datetime(msg.toe_wn, msg.toe_tow)
      t = self.base_ephemerides if from_base(msg) else self.rover_ephemerides
      m = exclude_fields(msg)
      m.update({'host_offset': host_offset, 'host_time': host_time})
      # For the moment, SITL and HITL analyses expect different
      # formats of ephemerides tables. Keep both until everyone's
      # migrated appropriately.
      if msg.healthy == 1 and msg.valid == 1:
        if time in self.ephemerides:
          self.ephemerides[time].update({msg.prn: m})
        else:
          self.ephemerides[time] = {msg.prn: m}
      if msg.prn in t:
        t[msg.prn].update({host_offset: m})
      else:
        t[msg.prn] = {host_offset: m}

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
      elif type(msg) is nav.MsgPosLLH:
        time = time_fn(self.time.wn, msg.tow / MSEC_TO_SECONDS)
        m['tow'] /= MSEC_TO_SECONDS
        self.rover_llh[time] = m
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
        d = walk_json_dict(s)
        d['host_offset'] = host_offset
        d['host_time'] = host_time
        if s.prn in self.rover_tracking:
          self.rover_tracking[s.prn].update({host_offset: d})
        else:
          self.rover_tracking[s.prn] = {host_offset: d}
      del m['states']

  def _process_iar(self, host_offset, host_time, msg):
    if type(msg) is piksi.MsgIarState:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      self.rover_iar_state[host_offset] = m

  def _process_log(self, host_offset, host_time, msg):
    if type(msg) is lg.MsgPrint:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      self.rover_logs[host_offset] = m

  def _process_thread_state(self, host_offset, host_time, msg):
    if type(msg) is piksi.MsgThreadState:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      m['cpu'] *= 0.1 # Scale from 1000. to 100.
      name = m['name'].rstrip('\x00')
      del m['name']
      if len(name) > 0:
        if name in self.rover_thread_state:
          self.rover_thread_state[name].update({host_offset: m})
        else:
          self.rover_thread_state[name] = {host_offset: m}

  def _process_uart_state(self, host_offset, host_time, msg):
    if type(msg) is piksi.MsgUartState:
      m = exclude_fields(msg)
      for i in ['uart_a', 'uart_b']:
        n = walk_json_dict(m[i])
        n['host_offset'] = host_offset
        n['host_time'] = host_time
        # Normalize to percentage from 255.
        n['rx_buffer_level'] = m[i]['rx_buffer_level'] / 255.
        n['tx_buffer_level'] = m[i]['tx_buffer_level'] / 255.
        if i in self.rover_uart_state:
          self.rover_uart_state[i].update({host_offset: n})
        else:
          self.rover_uart_state[i] = {host_offset: n}
      l = walk_json_dict(m['latency'])
      l['host_offset'] = host_offset
      l['host_time'] = host_time
      if 'latency' in self.rover_uart_state:
        self.rover_uart_state['latency'].update({host_offset: l})
      else:
        self.rover_uart_state['latency'] = {host_offset: l}

  def _process_acq(self, host_offset, host_time, msg):
    if type(msg) is acq.MsgAcqResult:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      if m['prn'] in self.rover_acq:
        self.rover_acq[m['prn']].update({host_offset: m})
      else:
        self.rover_acq[m['prn']] = {host_offset: m}

  def process_message(self, host_offset, host_time, msg):
    """Dispatches specific message types to the appropriate
    tables.

    Parameters
    ----------
    host_offset : int
      Millisecond offset since beginning of log.
    host_time : int
      Host UNIX epoch (UTC)
    msg : SBP message
      SBP message payload

    """
    self._process_acq(host_offset, host_time, msg)
    self._process_eph(host_offset, host_time, msg)
    self._process_iar(host_offset, host_time, msg)
    self._process_log(host_offset, host_time, msg)
    self._process_obs(host_offset, host_time, msg)
    self._process_pos(host_offset, host_time, msg)
    self._process_tracking(host_offset, host_time, msg)
    self._process_thread_state(host_offset, host_time, msg)
    self._process_uart_state(host_offset, host_time, msg)

  def save(self, filename):
    if os.path.exists(filename):
      print "Unlinking %s, which already exists!" % filename
      os.unlink(filename)
    f = pd.HDFStore(filename, mode='w')
    f.put('base_obs', pd.Panel(self.base_obs))
    f.put('base_obs_integrity', pd.DataFrame(self.base_obs_integrity))
    f.put('rover_obs', pd.Panel(self.rover_obs))
    f.put('rover_obs_integrity', pd.DataFrame(self.rover_obs_integrity))
    f.put('ephemerides', pd.Panel(self.ephemerides))
    f.put('rover_ephemerides', pd.Panel(self.rover_ephemerides))
    f.put('base_ephemerides', pd.Panel(self.base_ephemerides))
    f.put('rover_spp', pd.DataFrame(self.rover_spp))
    f.put('rover_llh', pd.DataFrame(self.rover_llh))
    f.put('rover_rtk_ned', pd.DataFrame(self.rover_rtk_ned))
    f.put('rover_rtk_ecef', pd.DataFrame(self.rover_rtk_ecef))
    f.put('rover_tracking', pd.Panel(self.rover_tracking))
    f.put('rover_iar_state', pd.DataFrame(self.rover_iar_state))
    f.put('rover_logs', pd.DataFrame(self.rover_logs))
    f.put('rover_thread_state', pd.Panel(self.rover_thread_state))
    f.put('rover_uart_state', pd.Panel(self.rover_uart_state))
    f.put('rover_acq', pd.Panel(self.rover_acq))
    f.close()


def hdf5_write(log_datafile, filename, verbose=False):
  processor = StoreToHDF5()
  i = 0
  logging_interval = 10000
  start = time.time()
  with JSONLogIterator(log_datafile) as log:
    for delta, timestamp, msg in log.next():
      i += 1
      if verbose and i % logging_interval == 0:
        print "Processed %d records! @ %.1f sec." % (i, time.time() - start)
      processor.process_message(delta, timestamp, msg)
    print "Processed %d records!" % i
    processor.save(filename)
  return filename


def main():
  """Fuck some Pandas

  """
  import argparse
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
        print "Processed %d records! @ %.1f sec." \
          % (i, time.time() - start)
      processor.process_message(delta, timestamp, msg)
      if num_records is not None and i >= int(num_records):
        print "Processed %d records!" % i
        break
    processor.save(filename)

if __name__ == "__main__":
  main()
