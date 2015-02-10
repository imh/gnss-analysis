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

#A fold to count the data points
class CountA(Analysis):
  def __init__(self, keep_as_map=False):
    super(CountA, self).__init__(
      key='count',
      keep_as_fold=True,
      keep_as_map=keep_as_map,
      fold_init=0)
  def compute(self, datum, current_analyses, prev_fold):
    return prev_fold['count'] + 1

class CountR(Report):
  def __init__(self):
    super(CountR, self).__init__(
      key='count',
      parents=set([CountA()]))
  def report(self, data, analyses, folds):
    return str(folds['count'])