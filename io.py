import pandas
import swiftnav.almanac as sa

__author__ = 'imh'

def load_yuma(yuma):
    yuma = yuma.readlines()
    almanacs = {}
    for n, line in enumerate(yuma):
        if line[:3] == "ID:":
            block = yuma[n:n+13]
            fields = map(lambda x: x[25:], block)

            prn     = int(fields[0])
            healthy = (int(fields[1]) == 0)
            ecc     = float(fields[2])
            toa     = float(fields[3])
            inc     = float(fields[4])
            rora    = float(fields[5])
            a       = float(fields[6])**2
            raaw    = float(fields[7])
            argp    = float(fields[8])
            ma      = float(fields[9])
            af0     = float(fields[10])
            af1     = float(fields[11])
            week    = int(fields[12])

            almanac = sa.Almanac(ecc, toa, inc, rora, a, raaw, argp, ma, af0, af1, week, prn, healthy)
            almanacs[prn] = almanac
    return almanacs


def load_data(data_filename):
    return pandas.read_hdf(data_filename)


def load_almanac(almanac_filename):
    return load_yuma(open(almanac_filename))