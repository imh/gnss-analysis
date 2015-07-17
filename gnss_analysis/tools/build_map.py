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

"""Command line utility for making KML files, etc.

"""

import argparse
import pandas as pd
import sys


def get_kml_doc(llhs):
  """Generates a KML document from a Pandas table of single point
  solutions. Requires columns lat, lon, and height.

  """
  from pykml.parser import Schema
  from pykml.factory import KML_ElementMaker as KML
  from pykml.factory import GX_ElementMaker as GX
  center = llhs[['lat', 'lon', 'height']].mean()
  elts = lambda e: '%s,%s,%d' % (e['lon'], e['lat'], e['height'])
  coords = ' '.join(llhs.apply(elts, axis=1).values)
  xml_coords = KML.coordinates(coords)
  doc = KML.kml(
          KML.Placemark(
            KML.name("gx:altitudeMode Example"),
            KML.LookAt(
              KML.longitude(center.lon),
              KML.latitude(center.lat),
              KML.heading(center.height),
              KML.tilt(70),
              KML.range(6300),
              GX.altitudeMode("relativeToSeaFloor"),),
            KML.LineString(
              KML.extrude(1),
              GX.altitudeMode("relativeToSeaFloor"),
              xml_coords
            )
          )
        )
  return doc

def write_kml(llhs, fname='file.kml'):
  """Generates and writes out a KML document from a Pandas table of
  single point solutions. Requires lat, lon, and height.

  """
  from lxml import etree
  doc = get_kml_doc(llhs)
  doc_str = etree.tostring(doc, pretty_print=True)
  with open(fname,'w+') as f:
    f.write(doc_str)

def main():
  parser = argparse.ArgumentParser(description='Swift Nav SBP log to HDF5 table tool.')
  parser.add_argument('file',
                      help='Specify the log file (in .hdf5) to use.')
  parser.add_argument('-k', '--kml',
                      action='store_true',
                      help='Output to KML.')
  args = parser.parse_args()
  log_datafile = args.file
  with pd.HDFStore(log_datafile) as store:
    try:
      if not store.rover_llh.empty:
        spp = store.rover_llh.T
        if args.kml:
          output_filename = log_datafile + '-all-spp.kml'
          print "Outputting spp, if available, to", output_filename
          write_kml(spp, output_filename)
        pseudo_spp = spp[spp.flags == 1]
        if args.kml and not pseudo_spp.empty:
          output_filename = log_datafile + '-pseudo-spp.kml'
          print "Outputting pseudo-absolute spp, if available, to", output_filename
          write_kml(pseudo_spp, output_filename)
      else:
        raise Exception("No single-point solutions available plotting.")
    except (KeyboardInterrupt, SystemExit):
      print "Exiting!"
      sys.exit()
    finally:
      store.close()

if __name__ == "__main__":
  main()
