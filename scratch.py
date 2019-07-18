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

    orders = [f for f in os.listdir(os.path.join(os.curdir, 'fb-eddm', 'production')) if f[-3:].upper() == 'DAT']
    dic = {}
    for order in orders:
        dic[order[:-4].split("_")[1]] = order

    print(dic)

    # sorted_dic = sorted(dic.items(), key=lambda kv: kv[0])
    sorted_dic = sorted(dic.items(), key=lambda kv: datetime.datetime.strptime(kv[0], "%Y%m%d%H%M%S"))

    files = [v for k, v in sorted_dic]
    for f in files:
        print(f)

    # sorted_dic = sorted(dic, key=operator.itemgetter('name'))
    # print(sorted_dic)

    # sorted_dic = collections.OrderedDict(dic)
    # print(sorted_dic)
    #
    # for v in sorted_dic:
    #     print(v)

    # for key, value in collections.OrderedDict(dic):
    #     print(key, value)



