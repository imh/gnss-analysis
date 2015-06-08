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
  return l[l > 0]


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


class LogAnnotator(object):
  """Given a HITL HDFSore, annotates key events and provides plotting
  help.

  """

  def __init__(self, hitl_log, events):
    self.events = dict([(e.name, e) for e in events])


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
  fig, axs = plt.subplots(1,2, sharey=False, figsize=(18,8))
  bounds = get_bounds(log.rover_llh.T[['lat', 'lon']])
  log.rover_llh.T[['lat','lon']].plot(kind='scatter',
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


def plot_stuff(hitl_log, start=None, end=None, ann_logs=True, plot_anns=[]):
  base_cn0 = hitl_log.base_obs[:, 'cn0', :].T
  rover_cn0 = hitl_log.rover_obs[:, 'cn0', :].T
  if start is None:
    start = rover_cn0.index[0]
  if end is None:
    end = base_cn0.index[-1]

  # Observations
  sdiff_L = get_sdiff('L', hitl_log.rover_obs, hitl_log.base_obs)
  ddiff_L = get_ddiff(get_ref_sat(sdiff_L), sdiff_L)
  sdiff_P = get_sdiff('P', hitl_log.rover_obs, hitl_log.base_obs)
  ddiff_P = get_ddiff(get_ref_sat(sdiff_L), sdiff_L)
  ddiff_L_t, ddiff_P_t = get_ddiff_t(ddiff_L), get_ddiff_t(ddiff_P)

  # Fixed RTK solution
  fixed = get_rtk_fixed(hitl_log)
  fixed_r = get_distances(fixed, loc.NOVATEL_BASELINE_1)

  # Float RTK solution
  float_pos = get_rtk_float(hitl_log)
  float_r = get_distances(float_pos, loc.NOVATEL_BASELINE_1)

  # SPP solution
  spp = get_spp(hitl_log)
  spp_r = get_distances(spp, loc.NOVATEL_ABSOLUTE_1)

  # IAR state
  iar_state = hitl_log.rover_iar_state.T['num_hyps']

  # Setup annotations
  anns = [('log_flash', mark_flash_saves(hitl_log)),
          ('log_trusted_eph', mark_new_trusted_ephs(hitl_log)),
          ('log_new_untrusted_ephs', mark_new_untrusted_ephs(hitl_log)),
          ('log_pvt', mark_pvt_warning(hitl_log)),
          ('log_soln_deadline', mark_soln_deadline(hitl_log)),
          ('log_iar', mark_iar(hitl_log)),
          ('log_iar_sats', mark_iar_add_sats(hitl_log)),
          ('log_errors', mark_errors(hitl_log)),
          ('log_warnings', mark_warnings(hitl_log)),
          ('fixed2float', mark_fixed2float(hitl_log)),
          ('obs_gaps', mark_obs_gaps(hitl_log)),
          ('large_fixed_error', mark_large_position_errors(fixed, loc.NOVATEL_BASELINE_1)),
          ('large_fixed_jump', mark_large_jumps(fixed, loc.NOVATEL_BASELINE_1)),
          ('large_float_error', mark_large_position_errors(float_pos, loc.NOVATEL_BASELINE_1)),
          ('large_float_jump', mark_large_jumps(float_pos, loc.NOVATEL_BASELINE_1)),
          ('large_spp_error', mark_large_position_errors(spp, loc.NOVATEL_ABSOLUTE_1)),
          ('large_spp_jump', mark_large_jumps(spp, loc.NOVATEL_ABSOLUTE_1))
         ]
  anns = dict(anns)

  # Plot stuff
  n_plots = 11
  n = 0
  fig, axs = plt.subplots(n_plots, 1, figsize=(16, 6*n_plots), sharex=True)
  if not fixed.truncate(start, end).empty:
    fixed.truncate(start, end).plot(ax=axs[n],
                                    title='HITL NED Fixed Baseline (meters)'); n+=1
    fixed_r.truncate(start, end).plot(ax=axs[n],
                                      title='HITL NED Baseline Magnitude Error (meters)'); n+=1
    fixed_r[fixed_r < 1500].truncate(start, end).plot(ax=axs[n],
                               title='HITL NED Baseline Magnitude Error (below 1000m) (meters)'); n+=1

  if not float_pos.truncate(start, end).empty:
    float_pos.truncate(start, end).plot(ax=axs[n],
                                        title='HITL NED Float Baseline (meters)'); n+=1
    float_r[float_r < 1500].truncate(start, end).plot(ax=axs[n],
                                                      title='HITL NED Baseline Magnitude Error (below 1000m) (meters)'); n+=1

  if not spp_r.truncate(start, end).empty:
    spp_r.truncate(start, end).plot(ax=axs[n],
                                    title='HITL SPP Magnitude Error (meters)'); n+=1

  if not sdiff_L.truncate(start, end).empty:
    sdiff_L.truncate(start, end).plot(ax=axs[n],
                                      title='SD Carrier Phase (cycles)'); n+=1
    ddiff_L_t.truncate(start, end).dropna(how='all', axis=1).plot(ax=axs[n],
                                        title='Smoothed DD Carrier Phase (cycles)'); n+=1

  if not rover_cn0.truncate(start, end).empty:
    rover_cn0.truncate(start, end).plot(ax=axs[n],
                                        title='HITL Rover Obs SNR'); n+=1
    base_cn0.truncate(start, end).plot(ax=axs[n],
                                       title='HITL Base Obs SNR'); n+=1

  if not iar_state.truncate(start, end).empty:
    iar_state.truncate(start, end).plot(ax=axs[n],
                                        title='Rover IAR State (hypotheses)'); n+=1

  # Logging message stuff
  logs = hitl_log.rover_logs.T['text'].truncate(start, end)
  if len(logs) > 100:
    print "Not annotating with logs, there are %d of them." % len(logs)
  elif ann_logs:
    print logs
    for ax in axs[:-1]:
      for log in logs.index:
        ax.axvline(log, linewidth=1, color='black', alpha=0.2)
    for i, log in zip(logs.index, logs):
      axs[n].text(i, 1, log, rotation='vertical', fontsize=8)
      axs[n].axis('off')
    n += 1

  # Show annotations
  for plot_ann in plot_anns:
    if anns.get(plot_ann, None):
      for ax in axs[:-1]:
        for v in anns[plot_ann].index:
          ax.axvline(log, linewidth=1, color='black', alpha=0.2)
    else:
      warnings.warn('Invalid key %.' % plot_ann)

  return anns


#####################################################################
## Extraneous data utilities

# TODO (Buro): Replace with a proper os.path'd version


def process_raw_log(date, verbose=False):
  local_dest = '/tmp/s3/'
  bucket_name = 'jenkins-backups-yz0bhivofjsjaieaebquxp'
  base_prefix = '/builds/'
  path = local_dest + bucket_name + base_prefix + "/" + date
  new_files = []
  gps_time_tabs = ['rover_iar_state', 'rover_logs', 'rover_tracking',
                   'rover_acq', 'rover_thread_state', 'rover_uart_state']
  for root, dirnames, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, 'serial*.json'):
      if verbose:
        print "Processing %s to hdf5" % filename
      nf = hdf5_write(root + "/" + filename,
                      root + "/" + filename + '.hdf5',
                      verbose)
      with pd.HDFStore(nf) as store:
        get_gps_time_col(store, gps_time_tabs, verbose=verbose)
        reindex_tables(store, ['rover_iar_state', 'rover_logs'], verbose=verbose)
      new_files.append(nf)
  return new_files


def get_from_s3(date,
                local_dest='/tmp/s3/',
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


def find_date(date, path='/tmp/s3/'):
  new_files = []
  for root, dirnames, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, 'serial*.hdf5'):
      if date in root:
        new_files.append(root + '/' + filename)
  return new_files
