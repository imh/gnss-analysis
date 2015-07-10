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

"""Interpolation and indexing utilities for Pandas tables. Most of
these are for indexing quantities (e.g., Piksi MsgPrint) that don't
have explicit GPS times.

"""

from gnss_analysis.stats_utils import truthify
from gnss_analysis.tools.records2table import hdf5_write
from pandas.tslib import Timestamp, Timedelta
import datetime
import fnmatch
import gnss_analysis.locations as loc
import matplotlib.pyplot as plt
import os
import numpy as np
import pandas as pd
import sys
import warnings

USEC_TO_SEC = 1e-6
MSEC_TO_SEC = 1e-3

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


#####################################################################
## Time indexing and interpolation for DataFrames and Panels


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


def get_gps_time_col(store, tabs, gpst_col='approx_gps_time', verbose=False):
  """Given an HDFStore and a list of tables in that HDFStore,
  interpolates GPS times for the desired tables and inserts the
  appropriate columns in the table.

  Parameters
  ----------
  store : pandas.HDFStore
    Pandas HDFStore
  gpst_col : str
    Key to insert new column
  tabs : list
    List of tables to interpolate for
  verbose : bool
    Verbose outoput

  """
  idx = store.rover_spp.T.host_offset.reset_index()
  model = interpolate_gpst_model(idx)
  init_date = store.rover_spp.T.index[0]
  f = lambda t1: apply_gps_time(t1*MSEC_TO_SEC, init_date, model)
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
      dft[gpst_col] = store[tab].T.host_offset.apply(f)
      store[tab] = dft.T
    elif isinstance(store[tab], pd.Panel):
      y = {}
      for prn in store[tab].items:
        y[prn] = store[tab][prn, 'host_offset', :].dropna().apply(f)
      ans = store[tab].transpose(1, 0, 2)
      ans['approx_gps_time'] = pd.DataFrame(y).T
      store[tab] = ans.transpose(1, 0, 2)


def reindex_tables(store, tabs, gpst_col='approx_gps_time', verbose=False):
  """"Reindexes the tables in an HDFStore using the specified column.

  Parameters
  ----------
  store : pandas.HDFStore
    Pandas HDFStore
  tabs : list
    List of tables to interpolate for
  gpst_col : str
    Key to insert new column

  """
  for tab in tabs:
    if verbose:
      print "Reindexing with approx_gps_time for %s." % tab
    if tab not in store:
      warnings.warn("%s not found in Pandas table" % tab, UserWarning)
    elif isinstance(store[tab], pd.DataFrame):
      store[tab] = store[tab].T.set_index(gpst_col).T
    elif isinstance(store[tab], pd.Panel):
      assert NotImplementedError

#####################################################################
## Anomaly detection


def find_largest_gaps(idx, n=10):
  """Given a time series index, finds the n largest gaps.  you may use
  this, for example, to find the time (endpoint) and duration of gaps
  in satellite observations.

  Parameters
  ----------
  idx : Pandas DatetimeIndex
  n : int
    n largest to return. Defaults to 10.

  Returns
  -------
  Pandas DatetimeIndex

  """
  adj =(idx - idx[0])/pd.Timedelta('00:00:01')
  return pd.Series(adj, idx).diff().nlargest(n)


def prefix_match_text(table, prefix, key='text'):
  return table.T[table.T[key].str.contains(prefix)]


def exact_match_text(table, contents, key='text'):
  return table.T[table.T[key] == contents]


#####################################################################
## Calculating quantities of interest.


def get_sdiff(obs_type, rover_obs,  base_obs):
  """For a given observation type, produces single-differenced
  observations from the base station and the rover.

  Parameters
  ----------
  obs_type : observation key, either 'P' (pseudorange) or 'L' (carrier phase)
  rover_obs : Panel of Rover observations
  base_obs : Panel of Base observations

  Returns
  -------
  DataFrame of single-difference observations

  """
  assert obs_type in ['P', 'L'], "Invalid obs_type: %s" % obs_type
  sdiff = rover_obs[:, obs_type, :] - base_obs[:, obs_type, :]
  return sdiff.dropna(how='all', axis=[0, 1]).T


def get_ref_sat(sdiff):
  """Given a set of single-difference observations, determine a
  reference satellite. By convention, this is the satellite with the
  most observations.

  Parameters
  ----------
  sdiff : DataFrame of single-difference observations.

  Returns
  -------
  int, prn of reference sat

  """
  return sdiff.count().argmax()


def get_observed_refsats(obs):
  """Returns an datetime index of observed satellites, based on
  time-series diffs of the pseudoranges.

  Parameters
  ----------
  obs : Pandas Panel of GPS observations.

  Returns
  -------
  Series, keyed by Datetime and prn of ref sat

  """
  vals = {}
  for sat, col in obs[:, 'P', :].T.diff().iteritems():
    c = col.dropna()[col.dropna() == 0]
    if not c.empty:
      vals[c.first_valid_index()] = sat
  return pd.Series(vals)


def get_ddiff(ref_sat, sdiff):
  """Given a reference satellite and sdiff observations, returns double
  difference observations.

  Parameters
  ----------
  ref_sat : int, reference satellite
  sdiff : DataFrame of single-difference observations.

  Returns
  -------
  Pandas DatetimeIndex

  """
  return sdiff - sdiff[ref_sat]


def get_ddiff_t(ddiff):
  """Produces "truthified" double-difference observation. Essentially a median
  filter/smoothing of the double-difference observations.

  Parameters
  ----------
  ddiff : DataFrame of double-differenced observations

  Returns
  -------
  Pandas DataFrame
  """
  return ddiff - truthify(ddiff)


def get_rtk_fixed(t):
  return t.rover_rtk_ned.T[t.rover_rtk_ned.T['flags'] == 1][['n', 'e', 'd']]


def get_rtk_float(t):
  return t.rover_rtk_ned.T[t.rover_rtk_ned.T['flags'] == 0][['n', 'e', 'd']]


def get_spp(t):
  return t.rover_spp.T[['x', 'y', 'z']]


def get_distances(df, loc):
  return np.sqrt(np.square(df - loc).sum(axis=1))


#####################################################################
## Log Annotations


class LogEvent(object):
  """ Basic container for log annotations.
  """

  def __init__(self, name, description, match):
    self.name = name
    self.description = description
    self.match = match

  def compute(self, t):
    self.result = self.match(t)
    return self.result


def mark_large_position_errors(t, loc, n=10, threshold=1000.):
  l = get_distances(t, loc).nlargest(n=n)
  return l[l > threshold]


def mark_large_jumps(t, loc, n=10, threshold=1000.):
  l = get_distances(t, loc).diff().nlargest(n=n)
  return l[l > threshold]


def mark_large_gaps(t, n=10, threshold=1.):
  l = find_largest_gaps(t.T.index, n=n)
  return l[l > threshold]


def mark_fixed2float(t):
  l = t.rover_rtk_ned.T[['flags']].diff().dropna()
  return l[l < 0].dropna()


def mark_flash_saves(t):
  return prefix_match_text(t.rover_logs, "INFO: Saved position to flash")


def mark_new_trusted_ephs(t):
  return prefix_match_text(t.rover_logs, "INFO: New trusted ephemeris for PRN")


def mark_new_untrusted_ephs(t):
  return prefix_match_text(t.rover_logs, "INFO: New untrusted ephemeris for PRN")


def mark_pvt_warning(t):
  return prefix_match_text(t.rover_logs, "WARNING: PVT warning")


def mark_soln_deadline(t):
  return prefix_match_text(t.rover_logs, "WARNING: Solution thread missed deadline")


def mark_iar(t):
  return prefix_match_text(t.rover_logs, "INFO: IAR:")


def mark_obs_gaps(t, threshold=1.):
  l = find_largest_gaps(t.rover_obs[:, 'cn0', :].T.index)
  return l[l > threshold]


def mark_iar_add_sats(t):
  return prefix_match_text(t.rover_logs, "INFO: add_sats")


def mark_errors(t):
  return prefix_match_text(t.rover_logs, "ERROR:")


def mark_warnings(t):
  return prefix_match_text(t.rover_logs, "WARNING:")


def mark_obs_matching(t):
  return prefix_match_text(t.rover_logs, "WARNING: Obs Matching:")


def mark_starting(t):
  return prefix_match_text(t.rover_logs, "Piksi Starting")


def mark_hardfaults(t):
  return prefix_match_text(t.rover_logs, "ERROR: HardFaultVector")


def mark_watchdog_reset(t):
  return prefix_match_text(t.rover_logs, "ERROR: Piksi has reset due to a watchdog timeout")


def mark_dgnss_baseline_warning(t):
  return prefix_match_text(t.rover_logs, "WARNING: dgnss_baseline")


def mark_old_ephemeris(t):
  return prefix_match_text(t.rover_logs, "WARNING: Using ephemeris older")


def mark_prn_tow_mismatch(t):
  return prefix_match_text(t.rover_logs, r'WARNING: PRN \d+ TOW mismatch')


def mark_null_acq_snr(t):
  return prefix_match_text(t.rover_logs, r'INFO: acq: PRN \d+ found @ 0 Hz, 0 SNR')


def mark_no_channels_free(t):
  return prefix_match_text(t.rover_logs, r'INFO: No channels free')


def mark_false_phase_lock(t):
  return prefix_match_text(t.rover_logs, r'WARNING: False phase lock')


def mark_ephemeris_diffs(ephemerides):
  ts = []
  for sat in ephemerides.items:
    diff = ephemerides[sat, :, :].T.dropna().diff()
    d = diff.T.drop(['host_offset', 'host_time', 'approx_gps_time']).T
    mask = d[(d.T != 0).any()].dropna()
    ts.append(ephemerides[sat, :, :].T.dropna().ix[mask.index.values])
  return pd.concat(ts).set_index('approx_gps_time').sort_index()


def mark_lock_cnt_diff(obs):
  df = obs[:, 'lock', :].T.diff()
  s = np.sqrt(np.square(df).sum(axis=1))
  return s[s > 0]


#####################################################################
## Plotting stuff


def get_center_pos(t):
  """Geodetic center point.
  """
  return tuple(t.rover_llh.T[['lat', 'lon']].mean())


def get_bounds(df):
  """Get a Cartesian bounding box on a Dataframe.

  """
  d = df.describe().T
  return [tuple(d['min']), tuple(d['max'])]


def build_map(t, loc, bounds):
  """Build a folium map centered at location with some bounds.

  """
  import folium
  map_ = folium.Map(location=loc,
                    tiles='Stamen Toner',
                    zoom_start=18)
  for i, row in t.iterrows():
    pos = [row['lat'], row['lon']]
    map_.simple_marker(location=pos, popup='x')
  return map_


def display_map(m, height=1000):
  """Takes a folium instance and embed HTML. From:
  https://github.com/python-visualization/folium/issues/90.

  """
  from IPython.display import HTML
  m._build_map()
  srcdoc = m.HTML.replace('"', '&quot;')
  embed = HTML('<iframe srcdoc="{0}" '
               'style="width: 100%; height: {1}px; '
               'border: none"></iframe>'.format(srcdoc, height))
  return embed


def display_log(t, limit=500):
  """Convenience method for displaying a Folium/javascript map of
  positions. Too many points will kill your browser session.

  """
  display_map(build_map(t.rover_llh.T[['lat', 'lon']].ix[0:limit, :],
                        loc=get_center_pos(t),
                        bounds=get_bounds(t)))


def plot_pos_parametric(log):
  """Parametric plot of position data from a testing log.

  """
  fig, axs = plt.subplots(1, 2, sharey=False, figsize=(18, 8))
  bounds = get_bounds(log.rover_llh.T[['lat', 'lon']])
  log.rover_llh.T[['lat', 'lon']].plot(kind='scatter',
                                       x='lat',
                                       y='lon',
                                       ylim=(bounds[0][1], bounds[1][1]),
                                       xlim=(bounds[0][0], bounds[1][0]),
                                       title="Rover SPP (degrees/LLH)",
                                       ax=axs[0])
  fixed = get_rtk_fixed(log)
  axs[1].set_title("Rover RTK Fixed(Blue) / Float(Red) (meters/NED)")
  if not fixed.empty:
    fixed.plot(kind='scatter', x='e', y='n', ax=axs[1])
  flt = get_rtk_float(log)
  if not flt.empty:
    flt.plot(kind='scatter', x='e', y='n', ax=axs[1], color='red')
  axs[1].vlines(x=0, ymin=fixed.min()['n'], ymax=fixed.max()['n'], color='black')
  axs[1].hlines(y=0, xmin=fixed.min()['e'], xmax=fixed.max()['e'], color='black')


def build_panel(rover_log, k, gpst_key='approx_gps_time'):
  buf = {}
  for item in rover_log.rover_thread_state.items:
    gpst_key = 'approx_gps_time'
    buf[item] = rover_log.rover_thread_state[item].T.set_index(gpst_key).dropna()[k]
  return (buf, list(rover_log.rover_thread_state.items))


def build_thread_state(rover_log):
  cpus = {}
  stacks = {}
  for item in rover_log.rover_thread_state.items:
    gpst_key = 'approx_gps_time'
    d = rover_log.rover_thread_state[item].T.set_index(gpst_key).dropna()
    cpus[item] = d['cpu']
    stacks[item] = d['stack_free']
  return (cpus, stacks, list(rover_log.rover_thread_state.items))


def plot_thread_state(cpus, stacks, threads, axs, interval):
  i, j = interval
  for thread in threads:
    cpus[thread].truncate(i, j).plot(ax=axs[0],
                                     title="CPU thread usage",
                                     label=thread,
                                     marker='.')
  axs[0].legend(axs[0].get_lines(), threads,
                loc='upper right',
                prop={'size': 8})
  axs[0].set_xlim(i, j)
  for thread in threads:
    stacks[thread].truncate(i, j).plot(ax=axs[1],
                                       title="CPU stack free",
                                       label=thread,
                                       marker='.')
  axs[1].legend(axs[1].get_lines(), threads,
                loc='upper right',
                prop={'size': 8})
  axs[1].set_xlim(i, j)


class Plotter(object):
  """
  """

  def __init__(self, hitl_log, ref_pos=(None, None), verbose=True):
    self.hitl_log = hitl_log
    self.verbose = verbose
    self.ref_rtk, self.ref_spp = ref_pos

  def interpolate(self):
    """
    """
    # Define start and end of time series around GPS observation SNR
    self.base_cn0 = self.hitl_log.base_obs[:, 'cn0', :].T/4.
    self.rover_cn0 = self.hitl_log.rover_obs[:, 'cn0', :].T/4.
    self.index = self.rover_cn0.index
    self.i = self.index[0]
    self.j = self.index[-1]
    if self.verbose:
      print "\nRover obs start @ %s and end @ %s.\n" % (self.i, self.j)
    self.sdiff_L = get_sdiff('L', self.hitl_log.rover_obs, self.hitl_log.base_obs)
    self.ddiff_L = get_ddiff(get_ref_sat(self.sdiff_L), self.sdiff_L)
    self.sdiff_P = get_sdiff('P', self.hitl_log.rover_obs, self.hitl_log.base_obs)
    self.ddiff_P = get_ddiff(get_ref_sat(self.sdiff_P), self.sdiff_P)
    self.ddiff_L_t = get_ddiff_t(self.ddiff_L)
    self.ddiff_P_t = get_ddiff_t(self.ddiff_P)
    # Fixed and Float RTK solution
    self.fixed = get_rtk_fixed(self.hitl_log)
    self.fixed_r = get_distances(self.fixed, self.ref_rtk)
    # Float RTK solution
    self.float_pos = get_rtk_float(self.hitl_log)
    self.float_r = get_distances(self.float_pos, self.ref_rtk)
    # SPP solution
    self.spp = get_spp(self.hitl_log)
    self.spp_r = get_distances(self.spp, self.ref_spp)
    # IAR state
    self.iar_state = self.hitl_log.rover_iar_state.T['num_hyps']

  def annotate(self):
    """
    """
    # Setup annotations
    anns = [('logs', self.hitl_log.rover_logs.T['text']),
            ('log_flash', mark_flash_saves(self.hitl_log)['text']),
            ('log_trusted_eph', mark_new_trusted_ephs(self.hitl_log)['text']),
            ('log_new_untrusted_ephs', mark_new_untrusted_ephs(self.hitl_log)['text']),
            ('log_pvt', mark_pvt_warning(self.hitl_log)['text']),
            ('log_soln_deadline', mark_soln_deadline(self.hitl_log)['text']),
            ('log_iar', mark_iar(self.hitl_log)['text']),
            ('log_iar_sats', mark_iar_add_sats(self.hitl_log)['text']),
            ('log_errors', mark_errors(self.hitl_log)['text']),
            ('log_warnings', mark_warnings(self.hitl_log)['text']),
            ('log_obs_matching', mark_obs_matching(self.hitl_log)['text']),
            ('log_starting', mark_starting(self.hitl_log)['text']),
            ('log_hardfault', mark_hardfaults(self.hitl_log)['text']),
            ('log_hardfault_unique', mark_hardfaults(self.hitl_log)['text']),
            ('log_watchdog', mark_watchdog_reset(self.hitl_log)['text']),
            ('log_dgnss_warnings', mark_dgnss_baseline_warning(self.hitl_log)['text']),
            ('log_prn_tow_mismatch', mark_prn_tow_mismatch(self.hitl_log)['text']),
            ('log_old_ephemeris', mark_old_ephemeris(self.hitl_log)['text']),
            ('fixed2float', mark_fixed2float(self.hitl_log)['flags']),
            ('log_null_acq_snr', mark_null_acq_snr(self.hitl_log)['text']),
            ('obs_gaps', mark_obs_gaps(self.hitl_log)),
            ('large_fixed_error', mark_large_position_errors(self.fixed, self.ref_rtk)),
            ('large_fixed_jump', mark_large_jumps(self.fixed, self.ref_rtk)),
            ('large_float_error', mark_large_position_errors(self.float_pos, self.ref_rtk)),
            ('large_float_jump', mark_large_jumps(self.float_pos, self.ref_rtk)),
            ('large_spp_error', mark_large_position_errors(self.spp, self.ref_spp, n=100)),
            ('large_spp_jump', mark_large_jumps(self.spp, self.ref_spp, n=100)),
            ('diff_ephemeris', mark_ephemeris_diffs(self.hitl_log.rover_ephemerides)['prn']),
            ('diff_rover_lock_cnt', mark_lock_cnt_diff(self.hitl_log.rover_obs)),
            ('diff_base_lock_cnt', mark_lock_cnt_diff(self.hitl_log.base_obs)),
            ('log_no_channels_free', mark_no_channels_free(self.hitl_log)['text']),
            ('log_false_phase_lock', mark_false_phase_lock(self.hitl_log)['text']),
            ('obs_refsat_rover', get_observed_refsats(self.hitl_log.rover_obs)),
            ('obs_refsat_base', get_observed_refsats(self.hitl_log.base_obs))]
    self.anns = dict(anns)
    sorted_anns = sorted(anns, key=lambda metric: len(metric[1]), reverse=True)
    if self.verbose:
      print "\n----- Annotations:"
      for n, ann in sorted_anns:
        print "{:22s} {:5d}".format(n, len(ann))
      print "-----\n"
    return self.anns

  def plot(self, interval=(None, None), ann_logs=True, plot_anns=[]):
    if all(interval):
      i, j = interval
    else:
      i, j = self.i, self.j
    if self.verbose:
      print "\nPlotting from %s to @ %s.\n" % (i, j)
    # Plot stuff
    targets = [(self.fixed, 'HITL Fixed Soln (meters/NED)'),
               (self.fixed_r, 'HITL Fixed Soln Magnitude Error (meters/NED)'),
               (self.fixed_r[self.fixed_r < 1500], 'HITL Fixed Soln Magnitude Error (below 1500m) (meters/NED)'),
               (self.float_pos, 'HITL Float Soln (meters/NED)'),
               (self.float_r, 'HITL Float Magnitude Error (meters/NED)'),
               (self.float_r[self.float_r < 1500], 'HITL Float Magnitude Error (below 1500m) (meters/NED)'),
               (self.spp_r, 'HITL SPP Soln Error (meters/ECEF)'),
               (self.sdiff_L, 'SD Carrier Phase (cycles)'),
               (self.ddiff_L_t, 'Smoothed DD Carrier Phase (cycles)'),
               (self.rover_cn0, 'HITL Rover Obs SNR'),
               (self.base_cn0, 'HITL Base Obs SNR'),
               (self.sdiff_P, 'SD Pseudorange (m)'),
               (self.ddiff_P_t, 'Smoothed DD Pseudorange (m)'),
               (self.iar_state, 'Rover IAR State (hypotheses)')
              ]
    n_plots = 1 + sum([int(not dat.truncate(i, j).empty) for (dat, t) in targets]) + 2
    if self.verbose:
      print "Number of plots %d between %s. and %s.\n" % (n_plots, i, j)
    fig, axs = plt.subplots(n_plots, 1, figsize=(16, 5*n_plots), sharex=False)
    n = 0
    for (dat, title) in targets:
      l = dat.truncate(i, j)
      if not l.empty:
        l.plot(ax=axs[n], title=title, marker='.')
        axs[n].get_xticklabels()[0].set_visible(True)
        axs[n].legend(loc='upper right')
        axs[n].set_xlim(i, j)
        n += 1
    # Show annotations
    for plot_ann in plot_anns:
      if plot_ann not in self.anns:
        warnings.warn('Invalid key %s.' % plot_ann)
      elif not self.anns[plot_ann].empty:
        self.anns[plot_ann] = self.anns[plot_ann].sort_index()
        g = self.anns[plot_ann][i:j]
        for ax in axs[:-3]:
          for v in g.index:
            ax.axvline(v, linewidth=1, color='black', alpha=0.6)
        for k in g.index:
          assert isinstance(self.anns[plot_ann], pd.Series), "Key %s not Series." % plot_ann
          s = "%s: %s" % (plot_ann, self.anns[plot_ann][k])
          axs[n].text(k, 1, s, rotation='vertical', fontsize=8)
        if self.verbose and not g.empty:
          print "\n%s ------" % plot_ann
          print self.anns[plot_ann].truncate(i, j)
          print "------"
    axs[-4].get_xaxis().set_visible(True)
    axs[n].set_title('Events')
    axs[n].spines['top'].set_visible(False)
    axs[n].spines['left'].set_visible(False)
    axs[n].spines['right'].set_visible(False)
    start = self.index[self.index.searchsorted(i)]
    end = self.index[self.index.searchsorted(j)]
    axs[n].set_xlim(start, end)
    # Plot CPU stuff
    cpus, stacks, threads = build_thread_state(self.hitl_log)
    plot_thread_state(cpus, stacks, threads, axs[-2:], interval=(i, j))


#####################################################################
## Extraneous data utilities

# TODO (Buro): Replace with a proper os.path'd version

DEFAULT_SWIFT_TMP_DIR = os.getenv('SWIFT_TMP',
                                  os.path.expanduser("~") + '/swift_tmp/s3/')

def process_raw_log(date,
                    local_dest=DEFAULT_SWIFT_TMP_DIR,
                    verbose=False):
  bucket_name = 'jenkins-backups-yz0bhivofjsjaieaebquxp'
  base_prefix = '/builds/'
  path = local_dest + bucket_name + base_prefix + "/" + date
  new_files = []
  gps_time_tabs = ['rover_iar_state', 'rover_logs', 'rover_tracking',
                   'rover_acq', 'rover_thread_state', 'rover_uart_state',
                   'rover_ephemerides', 'base_ephemerides']
  for root, dirnames, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, 'serial*.json'):
      if verbose:
        print "Processing %s to hdf5" % filename
      nf = hdf5_write(root + "/" + filename,
                      root + "/" + filename + '.hdf5',
                      verbose)
      with pd.HDFStore(nf) as store:
        if not store.rover_spp.empty:
          get_gps_time_col(store, gps_time_tabs, verbose=verbose)
          reindex_tables(store,
                         ['rover_iar_state', 'rover_logs'],
                         verbose=verbose)
      new_files.append(nf)
  return sorted(new_files)


def get_from_s3(date,
                local_dest=DEFAULT_SWIFT_TMP_DIR,
                access_key=os.getenv('AWS_ACCESS_KEY_ID', None),
                secret_key=os.getenv('AWS_SECRET_ACCESS_KEY', None),
                verbose=False):
  assert access_key is not None, 'Achtung! AWS_ACCESS_KEY_ID key must be set.'
  assert secret_key is not None, 'Achtung! AWS_SECRET_ACCESS_KEY key must be set.'
  from boto.s3.connection import S3Connection
  from boto.s3.key import Key
  from itertools import groupby
  bucket_name = 'jenkins-backups-yz0bhivofjsjaieaebquxp'
  base_prefix = 'builds/'
  bucket = S3Connection(access_key, secret_key).get_bucket(bucket_name)
  if verbose:
    print "Attempting to download dated log %s from S3:\n\n" % date
  for key in bucket.list(prefix=base_prefix + date):
      path = local_dest + bucket_name + "/" + key.name
      if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
      if not os.path.exists(path):
        if verbose:
          print "Downloading from S3 %s to %s " % (key, path)
        key.get_contents_to_filename(path)
      elif verbose:
        print "Damn! ... Snap! S3 %s already exists at %s" % (key, path)


def find_date(date, path=DEFAULT_SWIFT_TMP_DIR):
  new_files = []
  for root, dirnames, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, 'serial*.hdf5'):
      if date in root:
        new_files.append(root + '/' + filename)
  return sorted(new_files)


def get(log_date, verbose=False):
  get_from_s3(log_date, verbose)
  return process_raw_log(log_date, verbose)
