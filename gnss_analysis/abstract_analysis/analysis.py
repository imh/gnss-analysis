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

class Analysis(object):
  def __init__(self, key, parents=set(), keep_as_map=False, keep_as_fold=False, 
                     fold_init=None, is_summary=False):
    self.parents = parents
    self.key = key
    self.keep_as_map = keep_as_map
    self.keep_as_fold = keep_as_fold
    self.is_summary = is_summary
    self.fold_init = fold_init
    self.check_valid()

  def merge_storage(self, other):
    self.keep_as_map = self.keep_as_map or other.keep_as_map
    self.keep_as_fold = self.keep_as_fold or other.keep_as_fold
    self.is_summary = self.is_summary or other.is_summary

  def compute(self, data, current_analyses, prev_fold):
    pass

  def check_valid(self):
    #make sure that it has good properties
    valid = True
    # must be kept as map, fold, or summary
    valid = valid and (self.keep_as_map or self.keep_as_fold or self.is_summary)
    # cannot be map, fold, and summary
    valid = valid and not (self.is_summary and (self.keep_as_map or self.keep_as_fold))
    # TODO make sure there are no circular dependencies 
    #      (as of current code structure, shouldn't be possible to make one)
    if not valid:
      #TODO make better exceptions log
      raise Exception("Invalid analysis.")