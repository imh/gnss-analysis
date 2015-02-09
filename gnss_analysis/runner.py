

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
  return rem - loc

class DGNSSUpdater(object):
  def __init__(self, first_data_point, local_ecef_df, remote_ecef_df):
    self.local_ecef = determine_static_ecef(local_ecef_df)
    self.single_point_baseline = \
      guess_single_point_baselines(local_ecef_df, remote_ecef_df)
    mgmt.dgnss_init(first_data_point, self.local_ecef)
  def update_fun(self, datum)
    mgmt.dgnss_update(datum, self.local_ecef)

def main():
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
  