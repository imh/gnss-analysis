import numpy as np
import pandas as pd
import swiftnav.dgnss_management as mgmt
import utils
import swiftnav.coord_system as cs

__author__ = 'imh'


class Aggregator():
    def __init__(self, ecef, b, data):
        self.ecef = ecef
        self.b = np.array(b)

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

        # first_data_pt = data.ix[0]
        # self.sats = list(first_data_pt[~ (np.isnan(first_data_pt.L1) |
        #                                     np.isnan(first_data_pt.C1))].index)
        #TODO the aggregator only considers a single set of satellites, doesn't care that it's dynamic.
        # That means it's ignorant of the fact that the (real and tested) hypotheses sets may change

def analyze_datum(datum, i, time, ag):
    f2 = datum[~ (np.isnan(datum.L1) | np.isnan(datum.C1))]
    t_ = utils.datetime2gpst(time)  # TODO use a libswiftnav-python version
    sats = list(f2.index)
    measurements = np.concatenate(([f2.L1], [f2.C1]), axis=0).T
    numeric_sats = map(lambda x: int(x[1:]), list(sats))
    alms = [ag.alm[j] for j in numeric_sats]
    mgmt.dgnss_update(alms, t_,
                      measurements,
                      ag.ecef, 1)

    # get ILSQ ambiguity from 'known' baseline
    float_de = utils.get_de(ag.ecef + 0.5 * ag.b, ag.alm, numeric_sats, t_)  # TODO use the right ref and sats
    float_N_i = utils.get_N_from_b(measurements, float_de, ag.b)  # TODO second difference the measurements with the right ref
    #TODO save it in the Series or DataFrame output, along with its sats, so that we may analyze its variation and use dynamic sat sets

    #TODO the two baseline computations loop to find DE, which could be fused
    float_b = mgmt.measure_float_b(alms, t_, measurements, ag.ecef)
    resolved_b = mgmt.measure_b_with_external_ambs(alms, t_, measurements, float_N_i, ag.ecef)
    float_b = cs.wgsecef2ned(float_b, ag.ecef)  # NOTE: maybe use ag.ecef + 0.5*b or something
    resolved_b_NED = cs.wgsecef2ned(resolved_b, ag.ecef)
    ret = pd.Series(np.concatenate((float_b, resolved_b_NED)),
                    index=['float_b_N',    'float_b_E',    'float_b_D',
                           'resolved_b_N', 'resolved_b_E', 'resolved_b_D'])

    # check convergence/resolution
    num_hyps     = mgmt.dgnss_iar_num_hyps()
    num_hyp_sats = mgmt.get_iar_num_sats()

    float_converged = num_hyp_sats > 0
    resolution_done = float_converged and num_hyps == 1

    if float_converged:
        iar_de = utils.get_de(ag.ecef + 0.5 * ag.b, ag.alm, numeric_sats, t_)  # TODO use the right ref and sats
        iar_N_i = utils.get_N_from_b(measurements, iar_de, ag.b)  # TODO second_difference_the_measurements with the right ref
        #if the resolution just started
        if (not ag.resolution_started):
            ag.resolution_started = True
            # ag.resolution_contains_ilsq_N = mgmt.hyp_contains(float_N_i)  # TODO implement
            ag.float_convergence_time_delta = time - ag.t_0
            ag.float_convergence_i = i

        # if the integer ambiguity resolution just finished
        if ag.resolution_started and (not ag.resolution_ended) and resolution_done:
            n = mgmt.dgnss_iar_get_single_hyp()
            ag.resolution_ended = True
            ag.resolution_time_delta = time - ag.t_0
            ag.resolution_i = i
            ag.N = n
            ag.resolution_match = True
            for j in xrange(len(ag.N)):
                if int(ag.N[j]) != int(iar_N_i[j]):
                    ag.resolution_match = False

    return ret