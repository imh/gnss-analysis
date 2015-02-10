

from gnss_analysis.abstract_analysis.manage_tests import SITL
from gnss_analysis.data_io import load_sdiffs
import swiftnav.dgnss_management as mgmt

def determine_static_ecef(ecef_df):
  """
  Determine the static position of a receiver from a time series of it's 
  position in ECEF.
  TODO: Empirically, the mean works better than median, but we don't want to be
  succeptible to outliers, so we should eventually switch to a truncated mean.
  I would first try switching to LLH/local NED, take coordinate-wise medians, 
  then cut off the x percent of observation furthest from this position, and
  take the mean of the result in some rectangular frame (ECEF/NED).

  Parameters
  ----------
  ecef_df : DataFrame
    A time series of ECEF positions.

  Returns
  -------
  array
    An estimate of the receiver's position.
  """
  return np.array(ecef_df.mean(axis=0))

def guess_single_point_baselines(local_ecef_df, remote_ecef_df):
  """
  Use single point positions to estimate the baseline.

  Parameters
  ----------
  local_ecef_df : DataFrame
    A time series of single point ECEF positions of the local receiver.
  remote_ecef_df : DataFrame
    A time series of single point ECEF positions of the remote receiver.

  Returns
  -------
  array
    An estimate of the baseline (local minus remote).
  """
  loc = determine_static_ecef(local_ecef_df)
  rem = determine_static_ecef(remote_ecef_df)
  return loc - rem

def mk_swiftnav_sdiff(x):
  """
  Make a libswiftnav sdiff_t from an object with the same elements, 
  if possible, otherwise returning numpy.nan.
  We assume here that if C1 is not nan, then nothing else is nan,
  except potentially D1.

  Parameters
  ----------
  x : Series
    A series with all of the fields needed for a libswiftnav sdiff_t.

  Returns
  -------
  SingleDiff or numpy.nan
    If C1 is nan, we return nan, otherwise return a SingleDiff from the Series.
  """
    if np.isnan(x.C1) 
        return np.nan
    return SingleDiff(x.C1,
                      x.L1,
                      x.D1,
                      np.array([x.sat_pos_x, x.sat_pos_y, x.sat_pos_z]), 
                      np.array([x.sat_vel_x, x.sat_vel_y, x.sat_vel_z]),
                      x.snr,
                      x.prn)

class DGNSSUpdater(object):
  """
  Wraps an update function to be used by the SITL analysis.

  Parameters
  ----------
  first_data_point : DataFrame
    The first set of observations for initializing the filters.
  local_ecef_df : DataFrame
    A time series of single point ECEF positions of the local receiver.
  remote_ecef_df : DataFrame
    A time series of single point ECEF positions of the remote receiver.
  """
  def __init__(self, first_data_point, local_ecef_df, remote_ecef_df):
    self.local_ecef = determine_static_ecef(local_ecef_df)
    self.single_point_baseline = \
      guess_single_point_baselines(local_ecef_df, remote_ecef_df)
    mgmt.dgnss_init(first_data_point.apply(mk_swiftnav_sdiff, axis=0).dropna(), self.local_ecef)
  def update_fun(self, datum):
    """
    An state update function to be called by the SITL analyzer.

    Parameters
    ----------
    datum : DataFrame
      A DataFrame of data necessary to create a set of sdiff_t.
    """
    mgmt.dgnss_update(datum.apply(mk_swiftnav_sdiff, axis=0).dropna(), self.local_ecef)

def main():
  """
  Main entry point for running DGNSS SITL analysis.
  """
  import argparse
  parser = argparse.ArgumentParser(description='RTK Filter SITL tests.')
  parser.add_argument('file', help='Specify the HDF5 file to use.')
  args = parser.parse_args()
  hdf5_file = args.file
  
  data, local_ecef_df, remote_ecef_df = load_sdiffs_and_pos(hdf5_file)
  if len(data.items) < 2:
    raise Exception("Data must contain at least two observations.")
  first_datum = data.ix[0]
  data = data.ix[1:]

  updater = DGNSSUpdater(first_datum, local_ecef_df, remote_ecef_df)
  
  tester = SITL(updater.update_function, data)
  tester.add_report(Foo())
  
  reports = tester.compute()


if __name__ == "__main__"
  main()
  