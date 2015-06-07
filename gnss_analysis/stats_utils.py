#!/usr/bin/env python
# Copyright (C) 2015 Swift Navigation Inc.
# Contact: Ian Horn <ian@swiftnav.com>
#          Fergus Noble <fnoble@swiftnav.com>
#
# This source is subject to the license found in the file 'LICENSE' which must
# be be distributed together with this source. All other rights reserved.
#
# THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

import pandas as pd
import numpy as np


def truthifyv(phiv):
    runs = []
    current_run = []
    current_low = np.nan
    truth = np.empty(phiv.shape)
    truth[:] = np.nan
    for i, p in enumerate(phiv):
        if np.isnan(p):
            #then either just finished a run, or in the middle of a lull
            if not np.isnan(current_low):
                #then just finished a run
                runs.append((current_low, i, np.array(current_run)))
                current_low = np.nan
                current_run = []
        else:
            #then just starting a run or in the middle of one
            if np.isnan(current_low):
                #then just starting a run
                current_low = i
                current_run.append(p)
            else:
                #then in the middle of a run
                current_run.append(p)
    #finally may have ended in a run
    if not np.isnan(current_low):
        runs.append((current_low, i+1, np.array(current_run)))
    for low, high, run in runs:
        truth[low:high] = round(np.median(run))
    return truth


def truthify(phi):
    phiT = phi.values.T
    truth = np.empty(phiT.shape)
    truth[:,:] = np.nan
    for i in range(len(phiT)):
        truth[i] = truthifyv(phiT[i])
    return pd.DataFrame(truth.T,index=phi.index, columns=phi.columns)
