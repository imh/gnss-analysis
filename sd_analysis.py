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
        self.float_convergence_time_delta = None
        self.float_convergence_i = None

        self.resolution_ended = False
        self.N = None
        self.resolution_time_delta = None
        self.resolution_i = None
        self.resolution_match = False

        # first_data_pt = data.ix[0]
        # self.sats = list(first_data_pt[~ (np.isnan(first_data_pt.L1) |
        #                                     np.isnan(first_data_pt.C1))].index)
        #TODO the aggregator only considers a single set of satellites, doesn't care that it's dynamic

def analyze_datum(datum, i, time, ag):
    f2 = datum[~ (np.isnan(datum.L1) | np.isnan(datum.C1))]
    t_ = utils.datetime2gpst(time)  #TODO use a libswiftnav-python version
    sats = list(f2.index)
    numeric_sats = map(lambda x : int(x[1:]), list(sats))
    mgmt.dgnss_update([ag.alm[j] for j in numeric_sats], t_,
                np.concatenate(([f2.L1], [f2.C1]), axis=0).T,
                ag.ecef, 1)

    # get 'true' resolution from known baseline
    N_i = mgmt.get_N_from_b(ag.b)  #TODO implement
    #TODO save it in the Series or DataFrame output, along with its sats

    float_b_error = mgmt.get_float_b() - ag.b  #TODO implement
    resolved_b = mgmt.get_b_from_N(N_i)  #TODO implement
    float_b_error_NED = cs.wgsecef2ned(float_b_error, ag.ecef)
    resolved_b_NED = cs.wgsecef2ned(resolved_b, ag.ecef)
    ret = pd.Series(np.concatenate((float_b_error_NED, resolved_b_NED)),
                    index=['float_b_err_N', 'float_b_err_E', 'float_b_err_D',
                            'resolved_b_N',  'resolved_b_E',  'resolved_b_D'])

    # check convergence/resolution
    num_hyps = mgmt.get_ambiguity_num_hyps()  #TODO implement
    num_hyp_sats = mgmt.get_ambiguity_num_hyp_sats()  #TODO implement

    float_converged = num_hyp_sats > 0
    resolution_done = float_converged and num_hyps == 1

    #if the resolution just started
    if (not ag.resolution_started) and float_converged:
        ag.resolution_started = True
        ag.float_convergence_time_delta = time - ag.t_0
        ag.float_convergence_i = i

    # if the integer ambiguity resolution just finished
    if ag.resolution_started and (not ag.resolution_ended) and resolution_done:
        n = mgmt.get_ambiguity_n()  #TODO implement
        ag.resolution_ended = True
        ag.resolution_time_delta = time - ag.t_0
        ag.resolution_i = i
        ag.N = n
        ag.resolution_match = True
        for j in xrange(len(ag.N)):
            if int(ag.N[j]) != int(N_i[j]):
                ag.resolution_match = False

    return ret

