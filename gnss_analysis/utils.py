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

from swiftnav.single_diff import SingleDiff
import numpy as np
import pandas as pd
import swiftnav.lam as lam

def mk_swiftnav_sdiff(x):
  """
  Make a libswiftnav sdiff_t from an object with the same elements,
  if possible, otherwise returning numpy.nan.
  We assume here that if C1 is not nan, then nothing else is nan,
  except potentially D1.

  Parameters
  ----------
  x : Series
    A series with all of the fields needed for a libswiftnav sdiff_t.

  Returns
  -------
  SingleDiff or numpy.nan
    If C1 is nan, we return nan, otherwise return a SingleDiff from the Series.
  """
  if np.isnan(x.C1):
    return np.nan
  return SingleDiff(x.C1,
                    x.L1,
                    x.D1,
                    np.array([x.sat_pos_x, x.sat_pos_y, x.sat_pos_z]),
                    np.array([x.sat_vel_x, x.sat_vel_y, x.sat_vel_z]),
                    x.snr,
                    x.prn)

def sphere_b_covariance(var=0.0025):
    return np.eye(3, 3) * var


def dd_phi_cov(n, var):
    one_row = np.array([[1.0]*n])
    return (np.eye(n) + one_row.T.dot(one_row))*var


def get_N_from_b(phase, de, b, b_cov=None, phi_var=None):
    num_dds = de.shape[0]
    if b_cov is None:
        b_cov = sphere_b_covariance()
    if phi_var is None:
        phi_var = 9e-4
    phi_cov = dd_phi_cov(num_dds, phi_var)

    n_mean = phase - de.dot(b)/0.19029
    n_cov = phi_cov + de.dot(b_cov).dot(de.T)/0.19029/0.19029
    n = lam.ilsq(n_mean, n_cov, 1)[0]  # TODO verify shape
    return n


def normalize(x):
    return x/np.sqrt(x.dot(x))


l2pi = np.log(2*np.pi)


def neg_log_likelihood(x, sigma):
    #pdf(x) = (2*pi)**(-k/2) * abs(det(sigma))**(-0.5) * exp(-0.5 * x' * sigma**(-1) * x)
    #- ln(pdf(x)) = -[- k * 0.5 * ln(2*pi) - 0.5 * ln(abs(det(sigma))) - 0.5 * x' * sigma**(-1) * x ]
    #             = 0.5 * [ k * ln(2*pi) + ln(abs(det(sigma))) + x' * sigma**(-1) * x ]
    #- ln(pdf(x)) = -(-k/2) * ln(2*pi) + (-0.5)      *    ln(abs(det(sigma))) + (-0.5) * x' * sigma**(-1) * x
    #             =    k/2   *   ln(2*pi) + 0.5  *        ln(abs(det(sigma))) +   0.5  * x' * sigma**(-1) * x
    #             =    k * 0.5 * ln(2*pi) + 0.5  * 0.5  * ln(abs(det(sigma)))          * x' * sigma**(-1) * x
    #             =    k * gamma                        * ln(abs(det(sigma)))          * x' * sigma**(-1) * x
    #       where gamma = 0.5 *  ln(2*pi) * 0.5 * 0.5
    #                   = ln(2*pi) / 8
    log_abs_det_sig = np.log(abs(np.linalg.det(sigma)))
    k = len(x)
    pen = 0
    try:
        inv_sig_dot_x = np.linalg.solve(sigma, x)
    except np.linalg.linalg.LinAlgError:
        pinv_sig = np.linalg.pinv(sigma)
        inv_sig_dot_x = pinv_sig.dot(x)
        pen = 1e40
        log_abs_det_sig = 0

    return 0.5 * (k * l2pi + log_abs_det_sig + x.dot(inv_sig_dot_x)) + pen


def get_de(ref_ecef, alm, sats_w_ref_first, time):
    de = np.zeros((len(sats_w_ref_first)-1, 3))
    e0 = normalize(alm[sats_w_ref_first[0]].calc_state(time)[0] - ref_ecef)
    for i, sat in enumerate(sats_w_ref_first[1:]):
        de[i] = normalize(alm[sat].calc_state(time)[0] - ref_ecef) - e0
    return de


def not_nan(x):
    if str(x) == 'nan':
        return False
    return True

def get_non_nans(xs):
    return xs[xs.apply(not_nan)]

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