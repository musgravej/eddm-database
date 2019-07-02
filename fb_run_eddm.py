import dbfread
import dbf
import csv
import requests
import xml.etree.ElementTree as ET
import os
import math
import datetime
import shutil
import datetime
import configparser
import settings
import get_order_by_date

"""
This script will process FB EDDM lists downloaded from eddm order portal
"""
# TODO Add function that will open the production pdf and check the touch counts


def create_database(fle_path, fle, touch=''):

    newdb = os.path.join(fle_path, "{0}{1}".format(fle[:-4], touch))

    db = dbf.Table("{0}".format(newdb), ('FIRST C(25); ADDRESS C(1); CITY C(28); '
                                         'ST C(2); ZIP C(10); CRRT C(4); '
                                         'WALKSEQ_ C(7); STATUS_ C(1); '
                                         'BARCODE C(14); X C(1)')
                   )

    db_counts = dbf.Table("{0} CRRT Counts".format(newdb), ('ZIP C(6); CRRT C(5); RES C(6); POS C(5)'))

    db.open(mode=dbf.READ_WRITE)
    db_counts.open(mode=dbf.READ_WRITE)

    with open(os.path.join(fle_path, fle), 'r') as routes:
        csvr = csv.DictReader(routes, gblv.dat_header, delimiter='\t')
        next(csvr)
        for rec in csvr:
            addressee = gblv.record_addressee(rec['RouteID'])
            repeats = int(rec['Quantity'])
            db_counts.append((rec['ZipCode'], rec['RouteID'],
                              str(rec['Quantity']).zfill(5),
                              str(rec['POS']).zfill(5)), )

            for n in range(0, repeats):
                db.append((addressee, '',
                           rec['City'],
                           rec['State'],
                           rec['ZipCode'],
                           rec['RouteID'], '0000001', 'N',
                           '/{zip}{ckd}/'.format(zip=rec['ZipCode'], ckd=zip_ckd(rec['ZipCode'])),
                           ''), )

    db.close()
    db_counts.close()

    # Move copies to AccuZip folder
    shutil.copy(os.path.join(fle_path, "{0}{1}.dbf".format(fle[:-4], touch)),
                 os.path.join(gblv.accuzip_path, "{0}{1}.dbf".format(fle[:-4], touch)))


def process_dat(fle_path, fle):

    shutil.copy2(os.path.join(gblv.downloaded_orders_path, fle),
                 os.path.join(fle_path, fle))

    # get number of touches
    with open(os.path.join(fle_path, fle), 'r') as routes:
        csvr = csv.DictReader(routes, gblv.dat_header, delimiter='\t')
        next(csvr)
        for rec in csvr:
            gblv.touches = int(rec['NumberOfTouches'])
            break

    # If one touch, make one file
    if gblv.touches == 1:
        # print(fle)
        create_database(fle_path, fle)
        write_ini("{0}".format(fle[:-4]))

    # If two touches, make two files
    if gblv.touches == 2:
        for i, t in enumerate(['_1', '_2'], 1):
            create_database(fle_path, fle, t)
            write_ini("{0}".format(fle[:-4]), i)


def read_dbf(fle):
    # for rec in dbfread.DBF(fle):
    #     print(rec)

    db = dbfread.DBF('{fle}.dbf'.format(fle=fle))
    print(db.fields)
    # return
    for rec in db:
        print(rec)


def zip_ckd(zipcode):
    val = sum_digits(zipcode)
    return int(((math.ceil(val / 10.0)) * 10) - val)


def sum_digits(n):
    if not isinstance(n, (int,)):
        n = int(n)

    r = 0
    while n:
        r, n = r + n % 10, n // 10
    return r


def write_ini(fle, touch=False):
    gblv.set_touch_1_maildate(fle[-14:])
    gblv.set_touch_2_maildate()

    configfile = os.path.join(gblv.accuzip_path, 'mail_dates.ini')

    if not os.path.exists(configfile):
        config = configparser.ConfigParser()
        config.add_section('mailing_dates')
        with open(configfile, 'w') as c:
            config.write(c)

    config = configparser.ConfigParser()
    config.read(configfile)
    if touch:
        if touch == 1:
            config.set('mailing_dates', "{0}_{1}".format(fle, touch),
                       datetime.datetime.strftime(gblv.touch_1_maildate, "%m/%d/%Y"))
        if touch == 2:
            config.set('mailing_dates', "{0}_{1}".format(fle, touch),
                       datetime.datetime.strftime(gblv.touch_2_maildate, "%m/%d/%Y"))
    else:
        config.set('mailing_dates', fle, datetime.datetime.strftime(gblv.touch_1_maildate, "%m/%d/%Y"))

    with open(configfile, 'w') as c:
        config.write(c)


def download_web_orders():

    if gblv.environment == 'QA':
        token = gblv.fb_qa_token
    else:
        token = gblv.fb_token

    year = 2019

    month_start = 6
    day_start = 19

    month_end = 7
    day_end = 2

    date_start = (datetime.datetime.strptime("{y}-{m}-{d} 00:00:00".format(
                  m=month_start,y=year,d=str(day_start).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    date_end = (datetime.datetime.strptime("{y}-{m}-{d} 23:59:59".format(
                  m=month_end,y=year,d=str(day_end).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    get_order_by_date.order_request_by_date(date_start, date_end, gblv, token)
    get_order_by_date.clean_unused_orders(gblv, token)


def process_order(file):
    process_path = os.path.join(gblv.downloaded_orders_path, file[:-4])
    if os.path.exists(process_path):
        shutil.rmtree(process_path)
        os.mkdir(process_path)
    else:
        os.mkdir(process_path)

    process_dat(process_path, file)


if __name__ == '__main__':
    global gblv
    gblv = settings.GlobalVar()
    gblv.set_mailing_residential(True)

    # if not os.path.exists(gblv.db_name):
    #     get_order_by_date.intialize_databases(gblv)

    get_order_by_date.intialize_databases(gblv)
    download_web_orders()
    exit()

    orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']
    for order in orders:
        process_order(order)
        # os.remove(os.path.join(gblv.downloaded_orders_path, order))
