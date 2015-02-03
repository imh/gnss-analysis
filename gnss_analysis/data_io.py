
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
  

  Returns
  -------

  """
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
  

  Parameters
  ----------


  Returns
  -------
  
  """
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
  

  Parameters
  ----------


  Returns
  -------
  
  """
    ephs_before = filled_ephs[filled_ephs.items < t]
    if len(ephs_before.items) > 0:
        return ephs_before.ix[-1]
    else:
        return filled_ephs.ix[0]

def construct_pyobj_eph(eph):
  """
  

  Parameters
  ----------


  Returns
  -------
  
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

def mk_sdiff_series(eph, gpst, sd_obs, prn):
  """
  

  Parameters
  ----------


  Returns
  -------
  
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
  

  Parameters
  ----------


  Returns
  -------
  
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
  

  Parameters
  ----------


  Returns
  -------
  
  """
    s = pd.HDFStore(data_filename)
    if overwrite or not ('/'+key_sdiff) in s.keys():
      s[key_sdiff] = mk_sdiffs(s[key_eph], s[key_local], s[key_remote])
    sd = s[key_sdiff]
    s.close()
    return sd
