#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Ian Horn <ian@swiftnav.com>
#          Bhaskar Mookerji <mookerji@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

"""
Shared constants.

"""

# Units conversations (length, time, and 8-bit fractional cycles)
MAX_SATS = 32
MSEC_TO_SECONDS = 1000.
MM_TO_M = 1000.
CM_TO_M = 100.
Q32_WIDTH = 256.


# Constants and settings ported from Piksi firmware

# Solution constants
TIME_MATCH_THRESHOLD = 2e-3
OBS_PROPAGATION_LIMIT = 10e-3
MAX_AGE_OF_DIFFERENTIAL = 1.0
OBS_N_BUFF = 5

# Solution state
SOLN_MODE_LOW_LATENCY = 0
SOLN_MODE_TIME_MATCHED = 1
DGNSS_SOLUTION_MODE = SOLN_MODE_LOW_LATENCY

# RTK filter state
FILTER_FLOAT = 0
FILTER_FIXED = 1
dgnss_filter_state = FILTER_FLOAT

# RTK SHIT
MIN_SATS = 4

# Use Ephemeris from the last four hours
EPHEMERIS_TOL = 3600 * 4

# Constants from libswiftnav (include/libswiftnav/constants.h)
MAX_CHANNELS = 11
MAX_SATS = 32
GPS_C = 299792458.0
