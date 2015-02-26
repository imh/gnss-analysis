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

import sbp_log_analysis.metrics_schema as ms

class Report(object):
  """
  A report to be generated from SITL data.

  Parameters
  ----------
  key : str
    A key unique to each report used to identify it. (CANNOT BE EASILY CHECKED FOR UNIQUENESS)
  parents : set(Analysis)
    A set of Analysis objects on which the Report depends.
  dist_type : DistType
    The type of distribution to plot the report as, when aggregated across runs.
  """
  def __init__(self, key, parents, dist_type=ms.DistType.IGNORE):
    self.key = key
    self.parents = parents
    self.dist_type = dist_type


  def report(self, data, analyses, folds, parameters):
    """
    Generate a report from the data and analyses the Report depends on.

    Parameters
    ----------
    data : iterable
      The data passed through the analyses
    analyses : dict(str -> whatever)
      A map from Analysis keys to the results of map and summary analyses.
    folds : dict(str -> whatever)
      A map from Analysis keys to the final results of the fold analyses.
    parameters : object
      An object used to parametrize the SITL runs

    Returns
    -------
    printable
    """
    pass
