import pandas as pd
import numpy as np
import swiftnav.dgnss_management as mgmt
import swiftnav.coord_system as cs
import swiftnav.gpstime as gpstime
import dgnss_settings
import analysis_io
import utils
import sd_analysis
from scipy.optimize import minimize

__author__ = 'imh'

# load data

# process data:
#   for datum in data:
#     note time
#     note estimated baseline
#     attempt to use real baseline to get true ambiguity, note it
#     check convergence
#     if converged, note time, and whether converged correctly
class Analyzer():
    def __init__(self, b, ecef,
                 data_filename, data_key,
                 analysis_filename_prefix):
        # self.data = analysis_io.load_data(data_filename, data_key)
        # self.alm = analysis_io.load_almanac(almanac_filename)
        self.data = analysis_io.load_sdiffs(data_filename, data_key)
        self.settings = None
        self.b = b
        self.ecef = ecef

        self.nll_penalty = 1
        self.res_not_ended_penalty = 1e3
        self.res_not_started_penalty = 1e3
        self.ilsq_not_in_pool_penalty = 1e6
        self.not_res_to_ilsq_penalty = 1e6
        self.time_to_resolution_start_penalty = 0.5
        self.time_to_resolution_end_penalty = 1

        self.analysis_filename_prefix = analysis_filename_prefix
        self.iteration = 0

    def analyze_to_file(self, settings, analysis_filename):
        point_analyses, aggregate_analysis = self.run_analysis(settings)
        analysis_io.save_analysis(point_analyses, aggregate_analysis,
                                  settings, analysis_filename)

    def tune(self):
        x0 = self.initialize_x()
        res = minimize(self.objective_function, x0, method='nelder-mead',
                       options={'xtol': 1e-5, 'disp': True})
        return res.x

    def initialize_x(self):
        return np.array([9e-4 * 16, 100 * 400,
                         9e-4 * 16, 100 * 400,
                         1e-8,
                         1e8, 1e10])
        # return np.array([9e-4 * 16, 100 * 400,
        #                  9e-4 * 16, 100 * 400,
        #                  1e-1, 1e-5, 1e-8,
        #                  1e-8,
        #                  1e2, 4e2, 1e8,
        #                  1e10])

    def objective_function(self, x):
        settings = dgnss_settings.DgnssSettings(x[0], x[1],
                                                x[2], x[3],
                                                1e-1, 1e-5, 1e-8,
                                                x[4],
                                                1e2, 4e2, x[5],
                                                x[6])
        # print settings
        point_analyses, aggregate_analysis = self.run_analysis(settings)
        analysis_filename = self.analysis_filename_prefix + '_' + str(self.iteration) \
                            + '.hd5'
        self.iteration += 1
        analysis_io.save_analysis(point_analyses, aggregate_analysis,
                                  settings, analysis_filename)

        val = aggregate_analysis.kf_weighted_log_likelihood * self.nll_penalty
        if x[0] <= 0:
            val += 1e25
        if x[1] <= 0:
            val += 1e25
        if x[2] <= 0:
            val += 1e25
        if x[3] <= 0:
            val += 1e25
        if x[4] <= 0:
            val += 1e25
        if x[5] <= 0:
            val += 1e25
        if x[6] <= 0:
            val += 1e25
        print 'weighted nll = ' + str(aggregate_analysis.kf_weighted_log_likelihood)
        if aggregate_analysis.resolution_started:
            val += self.time_to_resolution_start_penalty \
                   * aggregate_analysis.float_convergence_time_delta.seconds
            print 'res started at ' + str(aggregate_analysis.float_convergence_time_delta)
            if not aggregate_analysis.resolution_contains_ilsq_N:
                val += self.ilsq_not_in_pool_penalty
                print 'no pool ilsq containment'
        else:
            val += self.res_not_started_penalty
            print 'res not started'
        if aggregate_analysis.resolution_ended:
            val += self.time_to_resolution_end_penalty \
                   * aggregate_analysis.resolution_time_delta.seconds
            print 'res ended at ' + str(aggregate_analysis.resolution_time_delta)
            if not aggregate_analysis.resolution_matches_ilsq_N:
                val += self.not_res_to_ilsq_penalty
                print 'no resolution ilsq match'
        else:
            val += self.res_not_ended_penalty
            print 'res not ended'

        print 'objective fun = ' + str(val) + '\n'
        return val

    def run_analysis(self, settings):
        # ecef = utils.get_ecef(data)  # TODO decide how we want to get this one

        point_analyses = {}
        aggregate_analysis = sd_analysis.Aggregator(self.ecef, self.b,
                                                    self.data, self.alm)
        if settings is not None:
            self.set_dgnss_settings(settings)
        self.initialize_c_code()

        for i, time in enumerate(self.data.items[1:]):
            print i
            point_analyses[time] = sd_analysis.analyze_datum(self.data.ix[time], i, time, aggregate_analysis)  #NOTE: this changes aggregate_analysis
        point_analyses = pd.DataFrame(point_analyses).T
        return point_analyses, aggregate_analysis

    def initialize_c_code(self):
        first_data_pt = self.data.ix[0][self.data.ix[0].apply(utils.not_nan)]
        sats = list(first_data_pt.index)
        numeric_sats = map(lambda x: int(x[1:]), sats)
        t0 = gpstime.datetime2gpst(self.data.index[0])

        mgmt.dgnss_init(first_data_pt,
                        self.ecef)

    def set_dgnss_settings(self, dgnss_settings):
        mgmt.set_settings(dgnss_settings.phase_var_test, dgnss_settings.code_var_test,
                          dgnss_settings.phase_var_kf, dgnss_settings.code_var_kf,
                          dgnss_settings.pos_trans_var, dgnss_settings.vel_trans_var, dgnss_settings.int_trans_var,
                          dgnss_settings.amb_drift_var,
                          dgnss_settings.pos_init_var, dgnss_settings.vel_init_var, dgnss_settings.amb_init_var,
                          dgnss_settings.new_int_var)


# def analyze(b, ecef, settings,
#             data_filename, data_key,
#             almanac_filename, analysis_filename):
#     point_analyses, aggregate_analysis =  run_analysis(b, ecef, settings,
#                                                        data_filename, data_key,
#                                                        almanac_filename)
#     analysis_io.save_analysis(point_analyses, aggregate_analysis, settings, analysis_filename)

# def run_analysis(b, ecef, settings,
#                  data_filename, data_key,
#                  almanac_filename):
#     data = analysis_io.load_data(data_filename, data_key)
#     alm = analysis_io.load_almanac(almanac_filename)
#
#     # ecef = utils.get_ecef(data)  # TODO decide how we want to get this one
#
#     point_analyses = {}
#     aggregate_analysis = sd_analysis.Aggregator(ecef, b, data, alm)
#
#     set_dgnss_settings(settings)
#
#     initialize_c_code(ecef, alm, data)
#
#     for i, time in enumerate(data.items[1:]):
#         point_analyses[time] = sd_analysis.analyze_datum(data.ix[time], i, time, aggregate_analysis)  #NOTE: this changes aggregate_analysis
#     point_analyses = pd.DataFrame(point_analyses).T
#     return point_analyses, aggregate_analysis

def main_tune():
    b = np.array([15, 0, 0])
    #rough lemnos position:
    llh = np.array([np.deg2rad(37.755927), np.deg2rad(-122.390699), 6])
    ecef = cs.wgsllh2ecef(*llh)
    data_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/Rover-20141125-232940-49.hd5"
    data_key = 'sdiff_rover_base'
    analysis_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/analysis.hd5"
    analysis_filename_prefix = "/users/imh/software/swift/libs/gnss-analysis/test-data/analyses"

    analyzer = Analyzer(b, ecef, data_filename, data_key, almanac_filename, analysis_filename_prefix)
    x = analyzer.tune()
    settings = dgnss_settings.DgnssSettings(x[0], x[1],
                                            x[2], x[3],
                                            1e-1, 1e-5, 1e-8,
                                            x[4],
                                            1e2, 4e2, x[5],
                                            x[6])
    print 'best found:'
    print settings

def main_analyze():
    b = np.array([15, 0, 0])
    #rough lemnos position:
    llh = np.array([np.deg2rad(37.755927), np.deg2rad(-122.390699), 6])
    ecef = cs.wgsllh2ecef(*llh)
    data_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/Rover-20141125-232940-49.hd5"
    data_key = 'sdiff_rover_base'
    analysis_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/analysis.hd5"
    analysis_filename_prefix = "/users/imh/software/swift/libs/gnss-analysis/test-data/analyses"

    analyzer = Analyzer(b, ecef, data_filename, data_key, almanac_filename, analysis_filename_prefix)
    x = analyzer.run_analysis(None)
    # x = analyzer.tune()

if __name__ == "__main__":
    main_analyze()

    # # b = np.array([-1.4861289,   0.84761746, -0.01029364])
    # # b = np.array([ 0.22566864, -1.22651958, -1.1712659 ])

    # b = np.array([15, 0, 0])
    # #rough lemnos position:
    # llh = np.array([np.deg2rad(37.755927), np.deg2rad(-122.390699), 6])
    # ecef = cs.wgsllh2ecef(*llh)
    # data_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/Rover-20141125-232940-49.hd5"
    # data_key = 'sdiff_rover_base'
    # analysis_filename = "/users/imh/software/swift/libs/gnss-analysis/test-data/analysis.hd5"
    # analysis_filename_prefix = "/users/imh/software/swift/libs/gnss-analysis/test-data/analyses"

    # analyzer = Analyzer(b, ecef, data_filename, data_key, almanac_filename, analysis_filename_prefix)
    # x = analyzer.tune()
    # settings = dgnss_settings.DgnssSettings(x[0], x[1],
    #                                         x[2], x[3],
    #                                         1e-1, 1e-5, 1e-8,
    #                                         x[4],
    #                                         1e2, 4e2, x[5],
    #                                         x[6])
    # print 'best found:'
    # print settings

    # # settings = dgnss_settings.DgnssSettings(phase_var_test=9e-4 * 16, code_var_test=100 * 400,
    # #                                         phase_var_kf=9e-4 * 16, code_var_kf=100 * 400,
    # #                                         pos_trans_var=1e-1, vel_trans_var=1e-5, int_trans_var=1e-8,
    # #                                         pos_init_var=1e2, vel_init_var=4e2, amb_init_var=1e8,
    # #                                         new_int_var=1e10)
    # #
    # # analyze(b, ecef, settings,
    # #         data_filename, data_key,
    # #         almanac_filename, analysis_filename)