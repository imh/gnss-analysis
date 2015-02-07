

from gnss_analysis.abstract_analysis.manage_tests import SITL
from gnss_analysis.data_io import load_sdiffs
import swiftnav.dgnss_management as mgmt

def init_fun(first_data_pt, ecef):
  mgmt.dgnss_init(first_data_pt, ecef)

class Updater(object):
  def __init__(self, ecef):
    self.ecef = ecef
  def update_fun(self, datum)
    mgmt.dgnss_update(datum, self.ecef)

def main():
  import argparse
  parser = argparse.ArgumentParser(description='RTK Filter SITL tests.')
  parser.add_argument('file',
                      help='Specify the HDF5 file to use.')
  args = parser.parse_args()
  hdf5_file = args.file
  
  data = load_sdiffs(hdf5_file)
  if len(data.items) < 2:
    raise Exception("Data must contain at least two observations.")
  first_datum = data.ix[0]
  data = data.ix[1:]

  #TODO get ECEF coordinates from somewhere
  init_fun(first_datum, ecef)
  updater = Updater(ecef)
  
  tester = SITL(updater.update_function, data)
  tester.add_report(Foo())
  
  reports = tester.compute()


if __name__ == "__main__"
  main()
  