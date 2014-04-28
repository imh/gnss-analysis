import numpy as np
import swiftnav.dgnss_management as mgmt
import io
import utils
import sd_analysis

__author__ = 'imh'

# load data

# process data:
#   for datum in data:
#     note time
#     note estimated baseline
#     attempt to use real baseline to get true ambiguity, note it
#     check convergence
#     if converged, note time, and whether converged correctly


def initialize_c_code(ecef, alm, data):

    first_data_pt = data.ix[0][~ (np.isnan(data.ix[0].L1) | np.isnan(data.ix[0].C1))]
    sats = list(first_data_pt.index)
    numeric_sats = map(lambda x: int(x[1:]), sats)
    t0 = data.items[0]

    mgmt.dgnss_init([alm[j] for j in numeric_sats], t0,
                    np.concatenate(([first_data_pt.L1],
                                    [first_data_pt.C1]),
                                   axis=0).T,
                    ecef, 1)

    # sats_man = mgmt.get_sats_management()
    # kf = mgmt.get_dgnss_kf()
    # stupid_state= mgmt.get_stupid_state(len(sats)-1)


def analyze(b, ecef, data_filename, almanac_filename, analysis_filename):
    data = io.load_data(data_filename)
    alm = io.load_almanac(almanac_filename)

    # ecef = utils.get_ecef(data)  # TODO decide how we want to get this one

    point_analyses = {}
    aggregate_analysis = sd_analysis.Aggregator(ecef, b, data)

    initialize_c_code(ecef, alm, data)

    for i, time in data.items:
        point_analyses[time] = sd_analysis.analyze_datum(data.ix[time], i, time, aggregate_analysis)  #NOTE: this changes aggregate_analysis

    io.save_analysis(point_analyses, aggregate_analysis, analysis_filename)  # TODO implement
