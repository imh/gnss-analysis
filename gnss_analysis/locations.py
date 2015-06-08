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

"""Some common locations. All approximate. Measured directly from
Piksi log data, and copied (!) from
sbp_log_analysis/locations.py. See:

https://github.com/swift-nav/sbp_log_analysis/blob/master/sbp_log_analysis/locations.py

"""

# Locations for Swift dual-antenna setup measured using SBP log
# analysis on Tuesday, 02 March 2015. Baseline measured directly from
# SBP log data. Absolute measurement is taken from a single-point
# solution data.
#
# Antenna source: Novatel Pinwheel (serial_link_log_20150303-103005.log)
NOVATEL_BASELINE = (0.108, 1.319, -0.201)         # Units: meters, NED
NOVATEL_ABSOLUTE = (-2704371, -4263206, 3884632)  # Units: meters, ECEF
# Antenna source: Leica Geosystems (serial_link_log_20150303-111106.log)
LEICA_BASELINE = (-0.108, -1.319, 0.201)
LEICA_ABSOLUTE = (-2704375, -4263211, 3884637)

# Locations for Swift dual-antenna setup measured using SBP log
# analysis on Friday, 22 May 2015. Baseline measured directly from SBP
# log data. Absolute measurement is taken from a single-point solution
# data. Uses:
# s3://jenkins-backups-yz0bhivofjsjaieaebquxp/builds/2015-05-22_20-39-31
# Antenna source: Novatel Pinwheel:
# 2015-05-22_20-39-31/archive/serial-link-20150522-131349.log.json
NOVATEL_BASELINE_1 = (0.164, 1.311, -0.197)         # Units: meters, NED
NOVATEL_ABSOLUTE_1 = (-2704371, -4263206, 3884632)  # Units: meters, ECEF
# Antenna source: Leica Geosystems (serial_link_log_20150303-111106.log)
# 2015-05-22_20-39-31/archive/serial-link-20150522-131351.log.json
LEICA_BASELINE_1 = (-0.164, -1.311, 0.197)
LEICA_ABSOLUTE_1 = (-2704376, -4263214, 3884641)
