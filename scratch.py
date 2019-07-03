import datetime
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

    ziplist = [55559, 55560, 55561, 55562, 55563, 55564,
               55565, 55566, 55567, 55568, 55569, 55570,
               55571, 55572, 55573, 55574, 55575, 55576,
               55577, 55578, 55473, 55474, 55478, 55479,
               55480, 55483, 55484, 55485, 55486, 55487,
               55488, 55550, 55551, 55552, 55553, 55554,
               55555, 55556, 55557, 55558, 55579, 55580,
               55581, 55582, 55583, 55584, 55585, 55586,
               55587, 55588, 55589, 55590, 55591, 55592,
               55593, 55594, 55595, 55596, 55597, 55598]

    usps_api.print_zip_search_results(ziplist)

    # mail_1 = touch_1_maildate('20190628210634')
    # mail_2 = touch_2_maildate(mail_1)

    # print("steven_morrissey_20190626170922.dat"[-18:-4])

