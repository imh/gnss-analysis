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

from collections import defaultdict
from itertools import chain
import gnss_analysis.runner as run
import numpy as np
import pandas as pd
import pytest


def approx_equal(x, y, tolerance=0.00000001):
  return abs(x - y) <= 0.5 * tolerance * (x + y)


def dict_approx_equal(x, y, tolerance=0.00000001):
  merged = defaultdict(list)
  for k, v in chain(x.iteritems(), y.iteritems()):
    merged[k].extend([v])
  return all([approx_equal(v0, v1) for (v0, v1) in merged.values()])


@pytest.mark.long
def test_sanity(capsys):
  """Basic integration test to validate that track regressions from
  refactoring. This test can take a while. Note that for the
  timebeing, the results here are actually really quite sensitive to

  """
  hdf5_filename = "data/serial-link-20150506-175750.log.json.new_fields.hdf5"
  is_ned = True
  measured_baseline = np.array([0.112, 1.317, -0.191])
  sitl_results = run.run(hdf5_filename, measured_baseline,
                         baseline_is_NED=is_ned)
  assert sitl_results['count'] == '7235'
  assert sitl_results['FixedIARBegun']
  assert not sitl_results['FixedIARLeastSquareEndedInPool']
  assert sitl_results['FixedIARCompleted']
  assert sitl_results['FixedIARLeastSquareStartedInPool']
  assert isinstance(sitl_results['FloatBaseline'], pd.Series)
  assert isinstance(sitl_results['FixedBaseline'], pd.Series)
  sitl_fix_soln = pd.DataFrame(np.array(list(sitl_results['FixedBaseline'])),
                               index=sitl_results['FixedBaseline'].index)
  described = sitl_fix_soln.describe().to_dict()
  assert described.keys() == [0, 1, 2]
  assert dict_approx_equal(described[0],
                           {'25%': 1.9192623486570597,
                            '50%': 1.9242509947049946,
                            '75%': 1.9288559278717152,
                            'count': 5649.0,
                            'max': 1.9443167011360045,
                            'mean': 1.9237216983756946,
                            'min': 1.89227432506871,
                            'std': 0.0072617625261221447})
