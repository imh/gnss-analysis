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
/base_ephemerides           wide         (shape->[1,28,10])
/rover_ephemerides          wide         (shape->[1,28,10])
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

# MsgPrints and MsgEphemeris sometimes coincide in time, so add a
# deterministic sequence count offset for each run of coincident logs.
SEQ_INTERVAL = 0.05

SPECIALLY_HANDLED_TYPES = [ob.MsgObs,
                           ob.MsgObsDepA,
                           ob.MsgEphemeris,
                           ob.MsgEphemerisDepA,
                           ob.MsgEphemerisDepB,
                           nav.MsgPosECEF,
                           nav.MsgPosLLH,
                           nav.MsgBaselineNED,
                           nav.MsgBaselineECEF,
                           tr.MsgTrackingState,
                           tr.MsgTrackingStateDepA,
                           piksi.MsgIarState,
                           lg.MsgLog,
                           lg.MsgPrintDep,
                           piksi.MsgThreadState,
                           piksi.MsgUartState,
                           acq.MsgAcqResult,
                           acq.MsgAcqResultDepA
                           ]

def _is_nested(attr):
  return len(attr.keys()) > 0 and isinstance(attr[attr.keys()[0]], dict)

def dict_depth(d, depth=0):
  if not isinstance(d, dict) or not d:
    return depth
  return max(dict_depth(v, depth + 1) for k, v in d.iteritems())

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
    self.eph_seq = 0
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
    self.log_seq = 0
    self.time = None
    self.generic_msgs = {}

  def _process_obs(self, host_offset, host_time, msg):
    if type(msg) in [ob.MsgObs, ob.MsgObsDepA]:
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
        prn = o.sid if msg.msg_type is ob.SBP_MSG_OBS else o.prn
        v = {'P': o.P / CM_TO_M, 'L': o.L.i + o.L.f / Q32_WIDTH,
             'cn0': o.cn0, 'lock': o.lock}
        v.update({'host_offset': host_offset, 'host_time': host_time})
        if time in t:
          t[time].update({prn: v})
        else:
          t[time] = {prn: v}
        # Set the 'counts' field such that the Nth bit is 1 iff we have
        # received a message whose 'count' field (the first byte of the n_obs
        # field) is N. If we have gotten them all, counts should be
        # (1 << total) - 1, and python makes the numbers really big as needed.
        if time in ti:
          ti[time].update({'counts':ti[time]['counts'] | 1 << count})
        else:
          ti[time] = {'total': total, 'counts': 1 << count}

  def _process_eph(self, host_offset, host_time, msg):
    if type(msg) in [ob.MsgEphemeris,
                     ob.MsgEphemerisDepA,
                     ob.MsgEphemerisDepB]:
      time = gpstime.gpst_components2datetime(msg.toe_wn, msg.toe_tow)
      t = self.base_ephemerides if from_base(msg) else self.rover_ephemerides
      prn = msg.sid if msg.msg_type is ob.SBP_MSG_EPHEMERIS else msg.prn
      m = exclude_fields(msg)
      m['host_time'] = host_time
      self.eph_seq = self.eph_seq + 1 if host_offset in self.ephemerides else 0
      m['host_offset'] = host_offset + SEQ_INTERVAL*self.eph_seq
      # For the moment, SITL and HITL analyses expect different
      # formats of ephemerides tables. Keep both until everyone's
      # migrated appropriately.
      if msg.healthy == 1 and msg.valid == 1:
        if time in self.ephemerides:
          self.ephemerides[time].update({prn: m})
        else:
          self.ephemerides[time] = {prn: m}
      if prn in t:
        t[prn].update({m['host_offset']: m})
      else:
        t[prn] = {m['host_offset']: m}

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
    if type(msg) in [tr.MsgTrackingState, tr.MsgTrackingStateDepA]:
      m = exclude_fields(msg)
      # Flatten a bit: reindex at the top level by prn and remove the
      # 'states' field from the message.
      for s in msg.states:
        d = walk_json_dict(s)
        prn = s.sid if msg.msg_type is tr.SBP_MSG_TRACKING_STATE else s.prn
        d['host_offset'] = host_offset
        d['host_time'] = host_time
        if prn in self.rover_tracking:
          self.rover_tracking[prn].update({host_offset: d})
        else:
          self.rover_tracking[prn] = {host_offset: d}
      del m['states']

  def _process_iar(self, host_offset, host_time, msg):
    if type(msg) is piksi.MsgIarState:
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      self.rover_iar_state[host_offset] = m

  def _process_log(self, host_offset, host_time, msg):
    if type(msg) in [lg.MsgLog, lg.MsgPrintDep]:
      m = exclude_fields(msg)
      self.log_seq = self.log_seq + 1 if host_offset in self.rover_logs else 0
      m['host_offset'] = host_offset + SEQ_INTERVAL*self.log_seq
      m['host_time'] = host_time
      self.rover_logs[m['host_offset']] = m

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
      for i in ['uart_a', 'uart_b' ,'uart_ftdi']:
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
    if type(msg) in [acq.MsgAcqResult, acq.MsgAcqResultDepA]:
      prn = msg.sid if msg.msg_type is acq.SBP_MSG_ACQ_RESULT else msg.prn
      m = exclude_fields(msg)
      m['host_offset'] = host_offset
      m['host_time'] = host_time
      if prn in self.rover_acq:
        self.rover_acq[prn].update({host_offset: m})
      else:
        self.rover_acq[prn] = {host_offset: m}

  def _process_generic(self, host_offset, host_time, msg):
    """
    Generically adds messages to the self.generic_msgs dict

    key for generic_msgs is the class name for the message

    Parameters
    ----------
    host_offset : int
      Millisecond offset since beginning of log.
    host_time : int
      Host UNIX epoch (UTC)
    msg : SBP message
      SBP message payload

    """
    m = exclude_fields(msg)
    m['host_offset'] = host_offset
    m['host_time'] = host_time
    key = msg.__class__.__name__
    msg_dict = self.generic_msgs.get(key, {})
    msg_dict.update({m['host_offset'] : m})
    self.generic_msgs[key] = msg_dict



  def process_message(self, host_offset, host_time, msg):
    """Dispatches specific message types to the appropriate
    tables.

    The SPECIALLY_HANDLED_TYPES list decides whether a msg
    is uniquely processed.  We'd like MsgGPSTime to be used
    for other message timestamps and to be stored as a table

    Parameters
    ----------
    host_offset : int
      Millisecond offset since beginning of log.
    host_time : int
      Host UNIX epoch (UTC)
    msg : SBP message
      SBP message payload

    """
    if type(msg) in SPECIALLY_HANDLED_TYPES + [nav.MsgGPSTime]:
      self._process_acq(host_offset, host_time, msg)
      self._process_eph(host_offset, host_time, msg)
      self._process_iar(host_offset, host_time, msg)
      self._process_log(host_offset, host_time, msg)
      self._process_obs(host_offset, host_time, msg)
      self._process_pos(host_offset, host_time, msg)
      self._process_tracking(host_offset, host_time, msg)
      self._process_thread_state(host_offset, host_time, msg)
      self._process_uart_state(host_offset, host_time, msg)
    else:
      self._process_generic(host_offset, host_time, msg)


  def save(self, filename):
    if os.path.exists(filename):
      print "Unlinking %s, which already exists!" % filename
      os.unlink(filename)
    try:
      f = pd.HDFStore(filename, mode='w')
      tabs = ['base_obs',
              'base_obs_integrity',
              'rover_obs',
              'rover_obs_integrity',
              'ephemerides',
              'rover_ephemerides',
              'base_ephemerides',
              'rover_spp',
              'rover_llh',
              'rover_rtk_ned',
              'rover_rtk_ecef',
              'rover_tracking',
              'rover_iar_state',
              'rover_logs',
              'rover_thread_state',
              'rover_uart_state',
              'rover_acq']
      for tab in tabs:
        attr = getattr(self, tab)
        if dict_depth(attr) == 3:
          f.put(tab, pd.Panel(attr))
        else:
          f.put(tab, pd.DataFrame(attr))
        if f.get(tab).empty:
          warnings.warn('%s is empty.' % tab)
      # For each generic message we add a column whose
      # name comes from the Msg's class name
      for eachkey in self.generic_msgs.iterkeys():
        msgdict = self.generic_msgs.get(eachkey, {})
        f.put(eachkey, pd.DataFrame(msgdict))
        if f.get(eachkey).empty:
           warnings.warn('%s is empty.' % eachkey)
    except:
      import traceback
      print traceback.format_exc()
    finally:
      f.close()


def hdf5_write(log_datafile, filename, verbose=False):
  processor = StoreToHDF5()
  i = 0
  logging_interval = 10000
  start = time.time()
  with JSONLogIterator(log_datafile) as log:
    for msg, data in log.next():
      i += 1
      if verbose and i % logging_interval == 0:
        print "Processed %d records! @ %.1f sec." % (i, time.time() - start)
      processor.process_message(data['delta'],  data['timestamp'], msg)
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
    for msg, data in log.next():
      i += 1
      if i % logging_interval == 0:
        print "Processed %d records! @ %.1f sec." \
          % (i, time.time() - start)
      processor.process_message(data['delta'], data['timestamp'], msg)
      if num_records is not None and i >= int(num_records):
        print "Processed %d records!" % i
        break
    processor.save(filename)

if __name__ == "__main__":
  main()
