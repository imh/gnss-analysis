

import pandas as pd
import numpy as np
from swiftnav.ephemeris import *
from swiftnav.single_diff import SingleDiff
from swiftnav.gpstime import *

def construct_pyobj_eph(eph):
    return Ephemeris(
               eph.tgd,
               eph.crs, eph.crc, eph.cuc, eph.cus, eph.cic, eph.cis,
               eph.dn, eph.m0, eph.ecc, eph.sqrta, eph.omega0, eph.omegadot, eph.w, eph.inc, eph.inc_dot,
               eph.af0, eph.af1, eph.af2,
               GpsTime(eph.toe_wn, eph.toe_tow), GpsTime(eph.toc_wn, eph.toc_tow),
               eph['valid'], # this syntax is needed because the method .valid takes precedence to the field
               eph.healthy,
               eph.prn+1) # +1 temporarily, until i get the next dataset where this is fixed

def separate_ephs(ephs):
    """
    Return a dictionary of prn to dataframe, where each dataframe is
    the unique ephemerides (unique and first, as in fst . groupby) over
    the time period the data was taken.
    """
    sep_ephs_tuples = [(int(prn),ephs[ephs['prn'] == prn]) for prn in ephs['prn'].unique()]
    sep_ephs = {}
    for sep_eph_tuple in sep_ephs_tuples:
        prn = sep_eph_tuple[0]+1 #temporarily, just for the dataset before i started storing them correctly TODO FIX
        frame = pd.DataFrame(sep_eph_tuple[1].drop_duplicates().apply(construct_pyobj_eph, axis=1), columns=['ephemeris'])
#         frame = pd.DataFrame(sep_eph_tuple[1].apply(construct_pyobj_eph, axis=1), columns=['ephemeris'])
        frame['time'] = frame.index
        sep_ephs[prn] = frame
    return sep_ephs

def merge_into_sdiffs(ephs, sd):
    """
    Taking ephemerides and observation data, this will merge them
    together into a panel whose index is a sat, major axis is time,
    and minor axis is everything needed for an sdiff struct.

    It's super slow, so I left it all in pandas format, so we can
    save it out in hdf5 and get it back all nicely processed.
    """
    sep_ephs = separate_ephs(ephs)
    sats = sd.items
    num_sats = map(lambda x: int(x[1:]),sats)
    sdiff_dict = {}
    for sat in sats:
    #     sat = sats[0]
        sat_ephs = sep_ephs[int(sat[1:])]
        fst_eph = sat_ephs.ix[0].ephemeris
        obs = sd[sat]
        obs['time'] = obs.index
        def make_single_diff(x):
            if np.isnan(x.C1) or np.isnan(x.L1) or np.isnan(x.S1_1) or np.isnan(x.S1_2):
                return pd.Series([np.nan]*11,
                                 index=['C1', 'L1', 'D1', 'sat_pos_x', 'sat_pos_y', 'sat_pos_z',
                                        'sat_vel_x', 'sat_vel_y', 'sat_vel_z', 'min_snr', 'prn'])
            c1 = x.C1
            l1 = x.L1
            snr = min(x.S1_1, x.S1_2)
            timestamp = x.time
            earlier_ephs = sat_ephs[sat_ephs['time'] <= timestamp]
            if earlier_ephs.shape[0] >= 1:
                eph = earlier_ephs.ix[-1].ephemeris
            else:
                eph = fst_eph
            gpstime = datetime2gpst(timestamp)
            pos, vel, clock_err, clock_rate_err = calc_sat_pos(eph, gpstime)
            return pd.Series([c1, l1, np.nan, pos[0], pos[1], pos[2], vel[0], vel[1], vel[2], snr, int(sat[1:])],
                             index=['C1', 'L1', 'D1', 'sat_pos_x', 'sat_pos_y', 'sat_pos_z',
                                    'sat_vel_x', 'sat_vel_y', 'sat_vel_z', 'min_snr', 'prn'])
        sdiffs = obs.apply(make_single_diff,axis=1).dropna(how='all',axis=0)
        sdiff_dict[sat] = sdiffs
    return pd.Panel(sdiff_dict)


def main():
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("ephemeris",
                        help="the ephemeris file to process")
    parser.add_argument("input",
                        help="the HDF5 file to process")
    parser.add_argument("base_name", default=False,
                        help="the marker name of the base station")
    parser.add_argument("rover_name", default=False,
                        help="the marker name of the rover")
    args = parser.parse_args()

    eph_file = pd.HDFStore(args.ephemeris)
    eph = eph_file['eph']

    h5 = pd.HDFStore(args.input)
    sd_table = h5['sd_%s_%s' % (args.rover_name, args.base_name)]

    output_table_name = 'sdiff_%s_%s' % (args.rover_name, args.base_name)
    h5[output_table_name] = merge_into_sdiffs(eph, sd_table)
    
    h5.close()

if __name__ == '__main__':
    main()


