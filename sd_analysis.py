import numpy as np
import pandas as pd
import swiftnav.dgnss_management as mgmt
import utils
import swiftnav.coord_system as cs
import operator

__author__ = 'imh'

class Aggregator():
    def __init__(self, ecef, b, data, alm):
        self.ecef = ecef
        self.b = np.array(b)
        self.b_NED = cs.wgsecef2ned(b, ecef)
        self.alm = alm

        self.t_0 = data.items[0]

        self.resolution_started = False
        self.resolution_contains_ilsq_N = None
        self.float_convergence_time_delta = None
        self.float_convergence_i = None

        self.resolution_ended = False
        self.N = None
        self.resolution_time_delta = None
        self.resolution_i = None
        self.resolution_matches_ilsq_N = None

        self.kf_weighted_log_likelihood = 0

        # first_data_pt = data.ix[0]
        # self.sats = list(first_data_pt[~ (np.isnan(first_data_pt.L1) |
        #                                     np.isnan(first_data_pt.C1))].index)
        #TODO the aggregator only considers a single set of satellites, doesn't care that it's dynamic.
        # That means it's ignorant of the fact that the (real and tested) hypotheses sets may change

    # def agg_map(self):
    #     return {'ecef_x': self.ecef[0],
    #             'ecef_y': self.ecef[1],
    #             'ecef_z': self.ecef[2],
    #             'b_N'   : self.b_NED[0],
    #             'b_E'   : self.b_NED[1],
    #             'b_D'   : self.b_NED[2],
    #             'self.t_0': self.t_0}

    def store(self, store_attrs):
        for k, v in self.__dict__.iteritems():
            try:
                setattr(store_attrs, k, v)
            except:
                print "Unable to store '%s'" % k

def analyze_datum(datum, i, time, ag):
    f2 = datum[~ (np.isnan(datum.L1) | np.isnan(datum.C1))]
    t_ = utils.datetime2gpst(time)  # TODO use a libswiftnav-python version
    sats = list(f2.index)
    measurements = np.concatenate(([f2.ix[sats,'L1']], [f2.ix[sats,'C1']]), axis=0).T
    numeric_sats = map(lambda x: int(x[1:]), list(sats))
    alms = [ag.alm[j] for j in numeric_sats]
    ref_ecef = ag.ecef.copy()
    if i == 728:
        ref_ecef += np.array([0,0, 1e-1])
    mgmt.dgnss_update(alms, t_,
                      measurements,
                      ref_ecef, 1)
    # mgmt.dgnss_update(alms, t_,
    #                   measurements,
    #                   ag.ecef + np.array([0, 0, 1e-1]), 1)

    # get ILSQ ambiguity from 'known' baseline
    float_de, float_phase = mgmt.get_float_de_and_phase(alms, t_, measurements, ag.ecef + 0.5 * ag.b)
    float_N_i_from_b = utils.get_N_from_b(float_phase, float_de, ag.b)
    #TODO save it in the Series or DataFrame output, along with its sats, so that we may analyze its variation and use dynamic sat sets

    #TODO the two baseline computations loop to find DE, which could be fused
    float_b = mgmt.measure_float_b(alms, t_, measurements, ag.ecef)
    faux_resolved_b = mgmt.measure_b_with_external_ambs(alms, t_, measurements, float_N_i_from_b, ag.ecef)
    float_b_NED = cs.wgsecef2ned(float_b, ag.ecef)  # NOTE: maybe use ag.ecef + 0.5*b or something
    faux_resolved_b_NED = cs.wgsecef2ned(faux_resolved_b, ag.ecef)

    # check convergence/resolution
    num_hyps     = mgmt.dgnss_iar_num_hyps()
    num_hyp_sats = mgmt.dgnss_iar_num_sats()

    float_converged = num_hyp_sats > 0
    resolution_done = float_converged and num_hyps == 1

    float_ambs = mgmt.get_amb_kf_mean()
    float_amb_cov = mgmt.get_amb_kf_cov(len(float_ambs))
    float_prns = mgmt.get_amb_kf_prns()
    float_ref_prn = float_prns[0]
    float_prns = float_prns[1:]

    cov_labels_cols = map(lambda x: 'float_amb_cov_' + str(x), float_prns)
    cov_labels = map(lambda col: map(lambda row: col + '_' + str(row), float_prns), cov_labels_cols)

    if not ag.resolution_ended:
        ag.kf_weighted_log_likelihood = \
            i / (i + 1.0) * ag.kf_weighted_log_likelihood \
          + utils.neg_log_likelihood(float_ambs - float_N_i_from_b,
                                     float_amb_cov) / (i + 1.0)

    output_data = np.concatenate((float_b_NED,
                                  faux_resolved_b_NED,
                                  [len(numeric_sats), num_hyps, num_hyp_sats],
                                  float_ambs,
                                  reduce(lambda x, y: np.concatenate((x, y)), float_amb_cov),
                                  float_N_i_from_b,
                                  [float_ref_prn]))
    output_labels = ['float_b_N',         'float_b_E',         'float_b_D',
                     'faux_resolved_b_N', 'faux_resolved_b_E', 'faux_resolved_b_D',
                     'num_sats', 'num_hyps', 'num_hyp_sats'] \
                  + map(lambda x: 'float_amb_' + str(x), float_prns) \
                  + reduce(operator.add, cov_labels) \
                  + map(lambda x: 'float_amb_from_b_' + str(x), float_prns) \
                  + ['float_ref_sat']

    if float_converged and (num_hyps > 0):
        iar_de, iar_phase = mgmt.get_iar_de_and_phase(alms, t_, measurements, ag.ecef + 0.5 * ag.b)
        iar_N_i_from_b = utils.get_N_from_b(iar_phase, iar_de, ag.b)
        iar_prns = mgmt.get_amb_test_prns()
        iar_ref_prn = iar_prns[0]
        iar_prns = iar_prns[1:]
        iar_faux_resolved_b = mgmt.measure_iar_b_with_external_ambs(alms, t_, measurements, iar_N_i_from_b, ag.ecef)
        iar_faux_resolved_b_NED = cs.wgsecef2ned(iar_faux_resolved_b, ag.ecef)
        iar_MLE_ambs = mgmt.dgnss_iar_MLE_ambs()
        iar_MLE_b = mgmt.measure_iar_b_with_external_ambs(alms, t_, measurements, iar_MLE_ambs, ag.ecef)
        iar_MLE_b_NED = cs.wgsecef2ned(iar_MLE_b, ag.ecef)

        output_data = np.concatenate((output_data,
                                      iar_MLE_b_NED,
                                      iar_faux_resolved_b_NED,
                                      iar_MLE_ambs,
                                      iar_N_i_from_b,
                                      [iar_ref_prn]))
        output_labels += ['iar_MLE_b_N', 'iar_MLE_b_E', 'iar_MLE_b_D']       \
                        +['iar_faux_resolved_b_N', 'iar_faux_resolved_b_E', 'iar_faux_resolved_b_D']       \
                        +map(lambda x: 'iar_MLE_amb_' + str(x), iar_prns)    \
                        +map(lambda x: 'iar_amb_from_b_' + str(x), iar_prns) \
                        +['iar_ref_sat']
        #if the resolution just started
        if (not ag.resolution_started):
            ag.resolution_started = True
            ag.resolution_contains_ilsq_N = (mgmt.dgnss_iar_pool_contains(iar_N_i_from_b) == 1)
            ag.float_convergence_time_delta = time - ag.t_0
            ag.float_convergence_i = i

        # if the integer ambiguity resolution just finished
        if ag.resolution_started and (not ag.resolution_ended) and resolution_done:
            n = mgmt.dgnss_iar_get_single_hyp(len(iar_N_i_from_b))
            ag.resolution_ended = True
            ag.resolution_time_delta = time - ag.t_0
            ag.resolution_i = i
            ag.N = n
            ag.resolution_matches_ilsq_N = True
            for j in xrange(len(ag.N)):
                if int(ag.N[j]) != int(iar_N_i_from_b[j]):
                    ag.resolution_matches_ilsq_N = False
                    break

    return pd.Series(output_data, index=output_labels)