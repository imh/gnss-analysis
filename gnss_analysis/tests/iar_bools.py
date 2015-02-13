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

# Is the actual IA vector in the set of hyps initially generated?
# Is the actual IA vector the one found at the end?

# We can answer another question to approximate this:
#   Assume we know an approximate baseline.
#   At each point in time, we can generate an integer least squares
#     estimate of the IA vector for some given set of satellites.
#   Either using the ILS estimate directly, or combining it with other
#     data points' ILS estimates, we come with some real estimate of the
#     IA vector.
#   Given an estimated IA vector, or some distribution thereof, we can
#     test whether it is in the hyp pool, or the distribution of whether
#     it is in the pool.
#
# Simplest version:
# At each data point:
#   has pool started?
#   If pool just started
#     ils_kf = ILS for KF's sats
#     if yes, fold True into remainder of data for 'ILS was in pool'
#     otherwise fold False
#     regardless fold that the pool started
#   has fixed rtk finished?
#   If IAR pool has already started and just finished:
#     ils_fixed = ILS for amb_test's sats
#     if final IAR solution matches ils_fixed, fold True to 'correct ILS found'

class FixedIARBegun(Analysis):
  """
  Keeps track of whether the fixed IAR process has begun yet in this dataset.
  (Fixed as opposed to float IAR.)
  """
  def __init__(self):
    super(FixedIARBegun, self).__init__(
      key='FixedIARBegun',
      keep_as_fold=True,
      fold_init=False)
  def compute(self, data, current_analyses, prev_fold, parameters):
    if prev_fold['FixedIARBegun']:
      return True
    return mgmt.dgnss_iar_num_sats() > 0 and mgmt.dgnss_iar_num_hyps() > 0

class FixedIARCompleted(Analysis):
  """
  Keeps track of whether the fixed IAR process has completed yet in this
  dataset. (Fixed as opposed to float IAR.)
  """
  def __init__(self):
    super(FixedIARCompleted, self).__init__(
      key='FixedIARCompleted',
      parents=set([FixedIARBegun()]),
      keep_as_fold=True,
      fold_init=False)
  def compute(self, data, current_analyses, prev_fold, parameters):
    if prev_fold['FixedIARCompleted']:
      return True
    return current_analyses['FixedIARBegun'] and mgmt.dgnss_iar_resolved()

class FixedIARLeastSquareInPool(Analysis):
  def __init__(self):
    super(FixedIARLeastSquareInPool, self).__init__(
      key='FixedIARLeastSquareInPool',
      parents=set([FixedIARBegun()]),
      keep_as_map=True)
  def compute(self, data, current_analyses, prev_fold, parameters):
    if current_analyses['FixedIARBegun']:
      dat = data.apply(ut.mk_swiftnav_sdiff, axis=0).dropna()
      iar_de, iar_phase = mgmt.get_iar_de_and_phase(
        dat, parameters.local_ecef + 0.5 * parameters.known_baseline)
      ia_vec_from_b = ut.get_N_from_b(iar_phase,
                                      iar_de,
                                      parameters.known_baseline)
      return mgmt.dgnss_iar_pool_contains(ia_vec_from_b)
    return None

class FixedIARLeastSquareStartedInPool(Analysis):
  def __init__(self):
    super(FixedIARLeastSquareStartedInPool, self).__init__(
      key='FixedIARLeastSquareStartedInPool',
      parents=set([FixedIARBegun(), FixedIARLeastSquareInPool()]),
      keep_as_fold=True,
      fold_init=False)
  def compute(self, data, current_analyses, prev_fold, parameters):
    if current_analyses['FixedIARBegun'] and not prev_fold['FixedIARBegun']:
      return current_analyses['FixedIARLeastSquareInPool']
    return prev_fold['FixedIARLeastSquareStartedInPool']

class FixedIARLeastSquareEndedInPool(Analysis):
  def __init__(self):
    super(FixedIARLeastSquareEndedInPool, self).__init__(
      key='FixedIARLeastSquareEndedInPool',
      parents=set([FixedIARCompleted(), FixedIARLeastSquareInPool()]),
      keep_as_fold=True,
      fold_init=False)
  def compute(self, data, current_analyses, prev_fold, parameters):
    if current_analyses['FixedIARCompleted'] and not prev_fold['FixedIARCompleted']:
      return current_analyses['FixedIARLeastSquareInPool']
    return prev_fold['FixedIARLeastSquareEndedInPool']

class FixedIARBegunR(Report):
  """
  Reports whether the fixed IAR process (hypothesis pool) ever started in this
  dataset.
  """
  def __init__(self):
    super(FixedIARBegunR, self).__init__(
      key='FixedIARBegun',
      parents=set([FixedIARBegun()]))
  def report(self, data, analyses, folds, parameters):
    return folds['FixedIARBegun']

class FixedIARCompletedR(Report):
  """
  Reports whether the fixed IAR process (hypothesis pool) ever completed in
  this dataset.
  """
  def __init__(self):
    super(FixedIARCompletedR, self).__init__(
      key='FixedIARCompleted',
      parents=set([FixedIARCompleted()]))
  def report(self, data, analyses, folds, parameters):
    return folds['FixedIARCompleted']

class FixedIARLeastSquareStartedInPoolR(Report):
  def __init__(self):
    super(FixedIARLeastSquareStartedInPoolR, self).__init__(
      key='FixedIARLeastSquareStartedInPool',
      parents=set([FixedIARLeastSquareStartedInPool()]))
  def report(self, data, analyses, folds, parameters):
    return folds['FixedIARLeastSquareStartedInPool']

class FixedIARLeastSquareEndedInPoolR(Report):
  def __init__(self):
    super(FixedIARLeastSquareEndedInPoolR, self).__init__(
      key='FixedIARLeastSquareEndedInPool',
      parents=set([FixedIARLeastSquareEndedInPool()]))
  def report(self, data, analyses, folds, parameters):
    return folds['FixedIARLeastSquareEndedInPool']