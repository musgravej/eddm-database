import dbfread
import dbf
import csv
import requests
import xml.etree.ElementTree as ET
import os
import math
import datetime
import shutil

"""
This script will process FB EDDM lists downloaded from eddm order portal
"""
# TODO Add function that will open the production pdf and check the touch counts

class GlobalVar():
    def __init__(self):
        self.mail_residential = False
        self.downloaded_orders_path = os.path.join(os.path.join(os.curdir, 'fb-eddm'))
        self.accuzip_path = os.path.join(self.downloaded_orders_path, 'accuzip_orders')
        self.create_accuzip_dir()
        self.touches = 0
        self.dat_header = ["AgentID", "DateSelected", "City", "State",
                           "ZipCode", "RouteID", "Quantity", "POS",
                           "NumberOfTouches"]

    def create_accuzip_dir(self):
        if not os.path.exists(self.accuzip_path):
            os.mkdir(self.accuzip_path)

    def set_mailing_residential(self, val):
        self.mail_residential = bool(val)

    def record_addressee(self, route):
        if route[0] == 'B':
            return 'PO BOX HOLDER'
        elif self.mail_residential:
            return 'RESIDENTIAL CUSTOMER'
        return 'POSTAL CUSTOMER'


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
        create_database(fle_path, fle)

    # If two touches, make two files
    if gblv.touches == 2:
        for i in ['_1', '_2']:
            create_database(fle_path, fle, i)


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
    gblv = GlobalVar()
    gblv.set_mailing_residential(True)

    orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']
    for order in orders:
        process_order(order)
        # os.remove(os.path.join(gblv.downloaded_orders_path, order))
