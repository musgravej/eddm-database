import datetime


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


if __name__ == '__main__':

    # mail_1 = touch_1_maildate('20190628210634')
    # mail_2 = touch_2_maildate(mail_1)

    print("steven_morrissey_20190626170922.dat"[-18:-4])

