import operator
import collections
import datetime
import os
import usps_api


def touch_2_maildate(file_date):
    proc_dt = file_date + datetime.timedelta(days=45)
    day_of_week = proc_dt.isoweekday()

    while day_of_week > 5:
        proc_dt = proc_dt + datetime.timedelta(days=1)
        day_of_week = proc_dt.isoweekday()

    print(proc_dt, datetime.datetime.strftime(proc_dt, "%A"))
    return proc_dt


def touch_1_maildate(file_date):
    proc_dt = datetime.datetime.strptime(file_date, "%Y%m%d%H%M%S")
    proc_dt = proc_dt + datetime.timedelta(days=5)
    day_of_week = proc_dt.isoweekday()

    while day_of_week > 5:
        proc_dt = proc_dt + datetime.timedelta(days=1)
        day_of_week = proc_dt.isoweekday()

    print(proc_dt, datetime.datetime.strftime(proc_dt, "%A"))
    return proc_dt


def usps_list_check(ziplist):
    pass


def dates():
    compare_dt = datetime.datetime.strptime('20190721043700', '%Y%m%d%H%M%S')
    print(datetime.datetime.utcnow())
    print(compare_dt)

    diff = datetime.datetime.utcnow() - compare_dt
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    print(hours)


if __name__ == '__main__':
    pass
