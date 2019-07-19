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


if __name__ == '__main__':

    datestring = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m%d_%H %M %p")
    print(datestring)
