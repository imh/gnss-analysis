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

from gnss_analysis.abstract_analysis.analysis import *
from gnss_analysis.abstract_analysis.report import *
import gnss_analysis.utils as ut
import swiftnav.dgnss_management as mgmt


class FloatBaselineA(Analysis):
  """
  The current float baseline
  """

  def __init__(self):
    k = 'FloatBaseline'
    super(FloatBaselineA, self).__init__(key=k, keep_as_map=True)

  def compute(self, data, current_analyses, prev_fold, parameters):
    d = data.apply(ut.mk_swiftnav_sdiff, axis=0).dropna(),
    num_used, b = mgmt.dgnss_new_float_baseline(d, parameters.rover_ecef)
    return b


class FloatBaselineR(Report):
  """
  A timeseries of the float baselines.
  """

  def __init__(self):
    k = "FloatBaseline"
    super(FloatBaselineR, self).__init__(key=k,
                                         parents=set([FloatBaselineA()]))

  def report(self, data, analyses, folds, parameters):
    return analyses['FloatBaseline']


class FixedBaselineA(Analysis):
  """
  The current fixed baseline, if we can make it, otherwise [nan, nan, nan]
  """

  def __init__(self):
    k = 'FixedBaseline'
    super(FixedBaselineA, self).__init__(key=k, keep_as_map=True)

  def compute(self, data, current_analyses, prev_fold, parameters):
    d = data.apply(ut.mk_swiftnav_sdiff, axis=0).dropna()
    num_used, b = mgmt.dgnss_fixed_baseline(d, parameters.rover_ecef)
    return b


class FixedBaselineR(Report):
  """
  A timeseries of the float baselines.
  """

  def __init__(self):
    k = "FixedBaseline",
    super(FixedBaselineR, self).__init__(key=k,
                                         parents=set([FixedBaselineA()]))

  def report(self, data, analyses, folds, parameters):
    return analyses['FixedBaseline']
