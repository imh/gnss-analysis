import datetime
import swiftnav.gpstime as gpstime
import swiftnav.lam as lam
import numpy as np

__author__ = 'imh'


def sphere_b_covariance(var=0.0025):
    return np.eye(3, 3) * var


def dd_phi_cov(n, var):
    one_row = np.array([[1.0]*n])
    return (np.eye(n) + one_row.T.dot(one_row))*var


def datetime2gpst(timestamp):
    dt = timestamp - datetime.datetime(1980, 1, 6, 0, 0, 0) + \
        datetime.timedelta(seconds=16)
    wn = dt.days / 7
    tow = (dt - datetime.timedelta(weeks=wn)).total_seconds()
    return gpstime.GpsTime(wn % 1024, tow)


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


def get_de(ref_ecef, alm, sats_w_ref_first, time):
    de = np.zeros((len(sats_w_ref_first)-1, 3))
    e0 = normalize(alm[sats_w_ref_first[0]].calc_state(time)[0] - ref_ecef)
    for i, sat in enumerate(sats_w_ref_first[1:]):
        de[i] = normalize(alm[sat].calc_state(time)[0] - ref_ecef) - e0
    return de