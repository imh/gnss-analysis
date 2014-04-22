import datetime
import swiftnav.gpstime as gpstime

__author__ = 'imh'


def datetime2gpst(timestamp):
    dt = timestamp - datetime.datetime(1980, 1, 6, 0, 0, 0) + datetime.timedelta(seconds=16)
    wn = dt.days / 7
    tow = (dt - datetime.timedelta(weeks=wn)).total_seconds()
    return gpstime.GpsTime(wn % 1024, tow)