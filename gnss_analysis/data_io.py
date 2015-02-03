
import pandas as pd
import numpy as np
from pynex.dd_tools import sds_with_lock_counts, sds
from swiftnav.ephemeris import *
from swiftnav.single_diff import SingleDiff
from swiftnav.gpstime import *


def get_fst_ephs(ephs):
  """
  Get a DataFrame containing the first non-NaN ephemerises for each sat.  

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
  """
  Fills in an ephemeris Panel so that there are no missing ephemerises.

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
  """
  Finds the most recent ephemeris before a given time.

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
  """
  Turns ephemeris data into a libswiftnav ephemeris.

  Parameters
  ----------
  eph : Series
    An ephemeris.

  Returns
  -------
  Ephemeris
    The same ephemeris as input, but with the libswiftnav Ephemeris type.
  """
  return Ephemeris(
             eph.tgd,
             eph.crs, eph.crc, eph.cuc, eph.cus, eph.cic, eph.cis,
             eph.dn, eph.m0, eph.ecc, eph.sqrta, eph.omega0, eph.omegadot, eph.w, eph.inc, eph.inc_dot,
             eph.af0, eph.af1, eph.af2,
             GpsTime(eph.toe_wn, eph.toe_tow), GpsTime(eph.toc_wn, eph.toc_tow),
             eph['valid'], # this syntax is needed because the method .valid takes precedence to the field
             eph.healthy,
             eph.prn)

def construct_pyobj_sdiff(s):
  """
  Turns sdiff data into the libswiftnav sdiff.

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

def mk_sdiff_series(eph, gpst, sd_obs, prn):
  """
  Makes a Series with all the fields needed for a sdiff_t, except doppler.

  Parameters
  ----------
  eph : Series
    An ephemeris applicable to this sat and time.
  gpst : GpsTime
    The time to compute sat pos/vel for.
  sd_obs : Series
    The single differenced observations.
  prn : int
    The prn for this sdiff

  Returns
  -------
  Series
    A series with the same fields as sdiff_t, except that doppler is NaN.  
  """
  pos, vel, clock_err, clock_rate_err = calc_sat_pos(construct_pyobj_eph(eph), gpst)
  return pd.Series([sd_obs['C1'], sd_obs['L1'], np.nan,
                    pos[0], pos[1], pos[2],
                    vel[0], vel[1], vel[2],
                    sd_obs['snr'], prn],
                   index=['C1', 'L1', 'D1',
                          'sat_pos_x', 'sat_pos_y', 'sat_pos_z',
                          'sat_vel_x', 'sat_vel_y', 'sat_vel_z',
                          'snr', 'prn'])

def mk_sdiffs(ephs, local, remote):
  """
  Computes everything needed for a timeseries of sdiff_t, dropping
  sats as appropriate (e.g. lock counts)

  Parameters
  ----------
  ephs : Panel
    The ephemerises to compute sat pos/vel from
  local : Panel
    The local receiver's observations
  remote : Panel
    The remote receiver's observations

  Returns
  -------
  Panel
    All the fields needed to construct sdiff_t's.
  """
  ephs = ephs.ix[:, [el for el in ephs.major_axis if el != 'payload'],:]
  obs = sds_with_lock_counts(local, remote)

  fst_ephs = get_fst_ephs(ephs)
  ephs = fill_in_ephs(ephs, fst_ephs)

  if not set(obs.minor_axis).issubset(set(fst_ephs.axes[1])):
    raise Exception("Not all sats with observations have ephemerises.")
  
  prev_lock1s = dict()
  prev_lock2s = dict()
  sdiffs = dict()
  for itm in obs.iteritems():
    t = itm[0]
    gpst = datetime2gpst(t)
    eph_t = get_timed_ephs(ephs, t)
    df = itm[1]
    current_lock1s = dict()
    current_lock2s = dict()
    sdiffs_now = dict()
    for sat in df.axes[1]:
      prev_lock1 = None
      prev_lock2 = None
      if sat in prev_lock1s:
        prev_lock1 = prev_lock1s[sat]
      if sat in prev_lock2s:
        prev_lock2 = prev_lock2s[sat]
      sd = df[sat]
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
        sdiffs_now[sat] = mk_sdiff_series(eph_t[sat], gpst, sd, sat)
    prev_lock1s = current_lock1s
    prev_lock2s = current_lock2s
    sdiffs[t] = pd.DataFrame(sdiffs_now)
  return pd.Panel(sdiffs)
  
def load_sdiffs(data_filename,
                key_eph='ephemerises', key_local='local', key_remote='remote',
                key_sdiff='sdiffs', overwrite=False):
  """
  Loads sdiffs from an HDF5 file, computing them if needed.

  Parameters
  ----------
  data_filename : str
    The filename of the HDF5 store with all the data.
  key_eph : str, optional
    The store's key for the ephemerises.
    (default 'ephemerises')
  key_local : str, optional
    The store's key for the local observations.
    (default 'local')
  key_remote : str, optional
    The store's key for the remote observations.
    (default 'remote')
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
  """
  s = pd.HDFStore(data_filename)
  if overwrite or not ('/'+key_sdiff) in s.keys():
    s[key_sdiff] = mk_sdiffs(s[key_eph], s[key_local], s[key_remote])
    # If a DataFrame of SingleDiffs is desired, use .apply(construct_pyobj_sdiff, axis=1).T
  sd = s[key_sdiff]
  s.close()
  return sd
