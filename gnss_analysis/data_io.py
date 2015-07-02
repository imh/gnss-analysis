#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Ian Horn <ian@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

import pandas as pd
import numpy as np
from pynex.dd_tools import sds_with_lock_counts
from swiftnav.ephemeris import *
from swiftnav.single_diff import SingleDiff
from swiftnav.gpstime import *
from swiftnav.pvt import calc_PVT
from swiftnav.track import NavigationMeasurement


def get_fst_ephs(ephs):
  """Get a DataFrame containing the first non-NaN ephemerises for each
  sat.

  Parameters
  ----------
  ephs : Panel
    and ephemeris Panel potentially containing NaN ephemerises

  Returns
  -------
  DataFrame
    A DataFrame whose colums are sats and rows are ephemeris fields.
    The fields are those of the first ephemeris in the input for which
    af0 is not NaN.

  """
  #TODO respect invalid/unhealthy ephemerises
  sats_to_be_found = set(ephs.minor_axis)
  fst_ephs = dict()
  for t in ephs.items:
    df = ephs.ix[t]
    for sat in sats_to_be_found:
      col = df[sat]
      if not np.isnan(col['af0']):
        fst_ephs[sat] = col
  return pd.DataFrame(fst_ephs)


def fill_in_ephs(ephs, fst_ephs):
  """Fills in an ephemeris Panel so that there are no missing
  ephemerises.

  Parameters
  ----------
  ephs : Panel
    A Panel of ephemerises with potentially missing colums
  fst_ephs : DataFrame
    A DataFrame of the first non-missing ephemerises for each satellite.

  Returns
  -------
  Panel
    The same panel as input, except the missing ephemerises are filled in with
    the most recent ephemeris if there is one, otherwise the first ephemeris.

  """
  #TODO respect invalid/unhealthy ephemerises
  new_ephs = ephs
  prev_eph = fst_ephs
  for itm in ephs.iteritems():
    t = itm[0]
    df = itm[1]
    for sat in df.axes[1]:
      if np.isnan(df[sat]['af0']):
        df[sat] = prev_eph[sat]
    prev_eph = df
    new_ephs[t] = df
  return new_ephs


def get_timed_ephs(filled_ephs, t):
  """Finds the most recent ephemeris before a given time.

  Parameters
  ----------
  filled_ephs : Panel
    A Panel of ephemerises with no missing colums
  t : datetime
    The time to lookup

  Returns
  -------
  DataFrame
    The most last ephemerises before t in filled_ephs.

  """
  ephs_before = filled_ephs[filled_ephs.items < t]
  if len(ephs_before.items) > 0:
    return ephs_before.ix[-1]
  else:
    return filled_ephs.ix[0]


def construct_pyobj_eph(eph):
  """Turns ephemeris data into a libswiftnav ephemeris.

  Parameters
  ----------
  eph : Series
    An ephemeris.

  Returns
  -------
  Ephemeris
    The same ephemeris as input, but with the libswiftnav Ephemeris type.

  """
  return Ephemeris(eph.tgd, eph.c_rs, eph.c_rc, eph.c_uc, eph.c_us,
                   eph.c_ic, eph.c_is, eph.dn, eph.m0, eph.ecc,
                   eph.sqrta, eph.omega0, eph.omegadot, eph.w,
                   eph.inc, eph.inc_dot, eph.af0, eph.af1, eph.af2,
                   GpsTime(eph.toe_wn, eph.toe_tow),
                   GpsTime(eph.toc_wn, eph.toc_tow),
                   # this syntax is needed because the method .valid takes
                   # precedence to the field
                   eph['valid'],
                   eph.healthy,
                   eph.prn)


def construct_pyobj_sdiff(s):
  """Turns sdiff data into the libswiftnav sdiff.

  Parameters
  ----------
  s : Series
    A single difference measurement and associated satellite positions.

  Returns
  -------
  SingleDiff
    The same single diff as input, but with the libswiftnav SingleDiff type.

  """
  if np.isnan(s.C1):
      return np.nan
  return SingleDiff(s.C1,
                    s.L1,
                    s.D1,
                    np.array([s.sat_pos_x, s.sat_pos_y, s.sat_pos_z]),
                    np.array([s.sat_vel_x, s.sat_vel_y, s.sat_vel_z]),
                    s.snr,
                    s.prn)


def mk_sdiff_series(sat_pos, sat_vel, sd_obs, prn):
  """Makes a Series with all the fields needed for a sdiff_t, except
  doppler.

  Parameters
  ----------
  sat_pos : array[3]
    The satellite position at the time of this observation
  sat_vel : array[3]
    The satellite velocity at the time of this observation
  sd_obs : Series
    The single differenced observations.
  prn : int
    The prn for this sdiff

  Returns
  -------
  Series
    A series with the same fields as sdiff_t, except that doppler is NaN.

  """
  return pd.Series([sd_obs['P'], sd_obs['L'], np.nan,
                    sat_pos[0], sat_pos[1], sat_pos[2],
                    sat_vel[0], sat_vel[1], sat_vel[2],
                    sd_obs['cn0'], prn],
                   index=['P', 'L', 'D1',
                          'sat_pos_x', 'sat_pos_y', 'sat_pos_z',
                          'sat_vel_x', 'sat_vel_y', 'sat_vel_z',
                          'snr', 'prn'])


def mk_sdiffs_and_abs_pos(ephs, rover_obs, base_obs):
  """Computes everything needed for a timeseries of sdiff_t, dropping
  sats as appropriate (e.g. lock counts), as well as absolute
  positions

  Parameters
  ----------
  ephs : Panel
    The ephemerises to compute sat pos/vel from
  rover_obs : Panel
    The rover receiver's observations
  base_obs : Panel
    The base receiver's observations

  Returns
  -------
  Panel
    All the fields needed to construct sdiff_t's.
  DataFrame
    The rover receiver's single point position.
  DataFrame
    The base receiver's single point position.

  """
  ephs = ephs.ix[:, [el for el in ephs.major_axis if el != 'payload'], :]
  obs = sds_with_lock_counts(rover_obs, base_obs)
  j = obs.transpose(1, 0, 2).join(
      rover_obs.transpose(1, 0, 2), rsuffix='_rover_obs').join(
      base_obs.transpose(1, 0, 2), rsuffix='_base_obs').transpose(1, 0, 2)
  fst_ephs = get_fst_ephs(ephs)
  ephs = fill_in_ephs(ephs, fst_ephs)
  j = j.ix[:, :, [sat for sat in fst_ephs.axes[1]]]
  if not set(j.minor_axis).issubset(set(fst_ephs.axes[1])):
    raise Exception("Not all sats with observations have ephemerises.")
  prev_lock1s = dict()
  prev_lock2s = dict()
  prev_carr_loc = dict()
  prev_carr_rem = dict()
  sdiffs = dict()
  ecef_loc = dict()
  ecef_rem = dict()
  receiver_positions = dict()
  prev_time = None
  for t, df in j.iteritems():
    gpst = datetime2gpst(t)
    eph_t = get_timed_ephs(ephs, t)
    current_lock1s = dict()
    current_lock2s = dict()
    current_carr_loc = dict()
    current_carr_rem = dict()
    dops_loc = dict()
    dops_rem = dict()
    sat_poss = dict()
    sat_vels = dict()
    clock_errs = dict()
    sdiffs_now = dict()
    for sat in df.axes[1]:
      sd = df[sat]
      sat_pos, sat_vel, clock_err, clock_rate_err =  \
        calc_sat_state(construct_pyobj_eph(eph_t[sat]), gpst)
      sat_poss[sat] = sat_pos
      sat_vels[sat] = sat_vel
      clock_errs[sat] = clock_err
      if not np.isnan(sd['L_rover_obs']):
        current_carr = sd['L_rover_obs']
        current_carr_loc[sat] = current_carr
        if sat in prev_carr_loc and not prev_time is None:
          prev_carr = prev_carr_loc[sat]
          dops_loc[sat] = (current_carr - prev_carr) / (t-prev_time).total_seconds()
      if not np.isnan(sd['L_base_obs']):
        current_carr = sd['L_base_obs']
        current_carr_rem[sat] = current_carr
        if sat in prev_carr_rem and not prev_time is None:
          prev_carr = prev_carr_rem[sat]
          dops_rem[sat] = (current_carr - prev_carr) / (t-prev_time).total_seconds()
      prev_lock1 = None
      prev_lock2 = None
      if sat in prev_lock1s:
        prev_lock1 = prev_lock1s[sat]
      if sat in prev_lock2s:
        prev_lock2 = prev_lock2s[sat]

      lock1 = sd['lock1']
      lock2 = sd['lock2']
      if not np.isnan(lock1):
        current_lock1s[sat] = lock1
      if not np.isnan(lock2):
        current_lock2s[sat] = lock2
      has_info = not (np.isnan(lock1) or np.isnan(lock2))
      lock1_good = (prev_lock1 is None) or (lock1 == prev_lock1)
      lock2_good = (prev_lock2 is None) or (lock2 == prev_lock2)
      if has_info and lock1_good and lock2_good:
        sdiffs_now[sat] = mk_sdiff_series(sat_pos, sat_vel, sd, sat)
    prev_lock1s = current_lock1s
    prev_lock2s = current_lock2s
    prev_carr_loc = current_carr_loc
    prev_carr_rem = current_carr_rem
    prev_time = t
    sdiffs[t] = pd.DataFrame(sdiffs_now)
    # TODO make a constants binding in libswiftnav-python
    ecef_loc[t] = pd.Series(compute_ecef((df.ix['P_rover_obs'] + pd.Series(clock_errs) * 299792458.0).dropna(),
                                         dops_loc, sat_poss, sat_vels, t),
                            index=['x', 'y', 'z'])
    ecef_rem[t] = pd.Series(compute_ecef((df.ix['P_base_obs'] + pd.Series(clock_errs) * 299792458.0).dropna(),
                                         dops_rem, sat_poss, sat_vels, t),
                            index=['x', 'y', 'z'])

  return pd.Panel(sdiffs), pd.DataFrame(ecef_loc).T, pd.DataFrame(ecef_rem).T


def compute_ecef(pseudoranges, dops, sat_poss, sat_vels, t):
  """
  Compute the receiver position if possible, otherwise a vector of NaNs.

  Parameters
  ----------
  pseudoranges : Series
    Pseudoranges, may have NaNs, but the elements for the keys which are also
    keys in dops, must not be NaN.
  dops : dict
    Dopplers, must not have NaNs.
  sat_poss : dict
    Satellite positions when this observation was received.
  sat_vels : dict
    Satellite velocities when this observation was received.

  Returns
  -------
  array
    The receiver position in ecef if there's enough info in the input to
    compute it, otherwise an array of NaNs.
  """
  if len(dops) < 4:
    return (np.nan, np.nan, np.nan)
  gpst = datetime2gpst(t)
  nms = []
  for itm in dops.iteritems():
    sat = itm[0]
    dop = itm[1]
    pseudorange = pseudoranges[sat]
    sat_pos = sat_poss[sat]
    sat_vel = sat_vels[sat]
    # TODO, make one of the pseudoranges/dops NaN or actually input and use it,
    # instead of using either the corrected/raw for both corrected and raw.
    # The magic number 1 is because the constructor needs an integer so can't
    # be NaN. We don't need that field so it can be anything.
    nms.append(NavigationMeasurement(pseudorange, pseudorange,
                                     np.nan, dop, dop, sat_pos, sat_vel,
                                     np.nan, np.nan, gpst, sat, 1))
  return calc_PVT(nms).pos_ecef


# Overwrite will overwrite if sdiffs are not in the table anyway
def load_sdiffs_and_pos(data_filename,
                        key_eph='ephemerides',
                        key_rover='rover_obs',
                        key_base='base_obs',
                        key_base_integrity='base_obs_integrity',
                        key_rover_ecef='rover_spp',
                        key_rover_integrity='rover_obs_integrity',
                        key_base_ecef='base_spp',
                        key_sdiff='sdiffs',
                        overwrite=False):
  """
  Loads sdiffs and single point positions from an HDF5 file,
  computing them if needed.

  Parameters
  ----------
  data_filename : str
    The filename of the HDF5 store with all the data.
  key_eph : str, optional
    The store's key for the ephemerises.
    (default 'ephemerises')
  key_rover : str, optional
    The store's key for the rover observations.
    (default 'rover')
  key_base : str, optional
    The store's key for the base observations.
    (default 'base')
  key_sdiff : str, optional
    The store's key for the single differenced observations.
    (default 'sdiffs')
  overwrite : bool, optional
    Whether to ignore existing sdiffs in key_sdiff and write new ones
    regardless. (default False)

  Returns
  -------
  Panel
    A Panel with everything needed to compute sdiff_t.
  DataFrame
    A timeseries of the single point positions of the rover receiver.
  DataFrame
    A timeseries of the single point positions of the base receiver.
  """
  s = pd.HDFStore(data_filename)
  if overwrite or not ('/' + key_sdiff) in s.keys() \
               or not ('/' + key_rover_ecef) in s.keys() \
               or not ('/' + key_base_ecef) in s.keys():
    bi = s[key_base_integrity]
    base_obs_good = bi.ix['counts']+1 == np.left_shift(1, bi.ix['total'])
    ri = s[key_rover_integrity]
    rover_obs_good = ri.ix['counts']+1 == np.left_shift(1, ri.ix['total'])
    s[key_sdiff], s[key_rover_ecef], s[key_base_ecef] = \
      mk_sdiffs_and_abs_pos(s[key_eph],
                            s[key_rover].ix[rover_obs_good],
                            s[key_base].ix[base_obs_good])
    # If a DataFrame of SingleDiffs is desired, use .apply(construct_pyobj_sdiff, axis=1).T
  sd = s[key_sdiff]
  rover_ecef = s[key_rover_ecef]
  base_ecef = s[key_base_ecef]
  s.close()
  return sd, rover_ecef, base_ecef
