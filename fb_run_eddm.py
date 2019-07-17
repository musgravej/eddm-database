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
import sqlite3

"""
This script will process FB EDDM lists downloaded from eddm order portal
"""
# TODO Add function that will open the production pdf and check the touch counts


def create_database(eddm_order, fle_path, db_name, order_file):
    """
    Writes the eddm job db file used by accuzip
    Saves a copy in the the job job folder, and saves a copy to the accuzip folder

    :param eddm_order: eddm order object
    :param fle_path: path to save the files to
    :param db_name: name of new db file
    :param order_file: name of the original order file
    :return:
    """
    full_newdb_path = os.path.join(fle_path, db_name)

    db = dbf.Table("{0}".format(full_newdb_path), ('FIRST C(25); ADDRESS C(1); CITY C(28); '
                                         'ST C(2); ZIP C(10); CRRT C(4); '
                                         'WALKSEQ_ C(7); STATUS_ C(1); '
                                         'BARCODE C(14); X C(1)')
                   )

    db_counts = dbf.Table("{0} CRRT Counts".format(full_newdb_path), ('ZIP C(6); CRRT C(5); RES C(6); POS C(5)'))

    db.open(mode=dbf.READ_WRITE)
    db_counts.open(mode=dbf.READ_WRITE)

    with open(os.path.join(gblv.downloaded_orders_path, order_file), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        for rec in csvr:
            addressee = eddm_order.record_addressee(rec['RouteID'])
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

    shutil.copy(os.path.join(fle_path, "{}.dbf".format(db_name)),
                os.path.join(gblv.accuzip_path, "{}.dbf".format(db_name)))


def write_azzuzip_files(eddm_order, fle_path, fle, match_search):

    # If one touch, make one file
    if eddm_order.file_touches == 1:
        insert_values = {'filename': fle, 'jobname': match_search[2],
                         'processing_date': datetime.datetime.now(),
                         'order_records': eddm_order.file_qty,
                         'total_touches': eddm_order.file_touches,
                         'touch': 1 ,'mailing_date': eddm_order.touch_1_maildate,
                         'user_id': match_search[8]}

        # Write eddm order db file
        create_database(eddm_order, fle_path, insert_values['jobname'], fle)
        # Insert order into FileHistory table
        get_order_by_date.update_file_history_table(gblv, **insert_values)
        # Update ini file in accuzip folder
        write_ini(eddm_order, insert_values['jobname'], insert_values['mailing_date'])

    # If two touches, make two files
    if eddm_order.file_touches == 2:
        for i, t in enumerate(['_1', '_2'], 1):
            insert_values = {'filename': "{0}{1}.dat".format(fle[:-4], t),
                             'jobname': "{0}_{1}".format(match_search[2], i),
                             'processing_date': datetime.datetime.now(),
                             'order_records': eddm_order.file_qty,
                             'total_touches': eddm_order.file_touches,
                             'touch': i,
                             'mailing_date': {1: eddm_order.touch_1_maildate, 2: eddm_order.touch_2_maildate}[i],
                             'user_id': match_search[8]}

            # Write eddm order db file
            create_database(eddm_order, fle_path, insert_values['jobname'], fle)
            # Insert order into FileHistory table
            get_order_by_date.update_file_history_table(gblv, **insert_values)
            # Update ini file in accuzip folder
            write_ini(eddm_order, insert_values['jobname'], insert_values['mailing_date'])


def process_dat(fle):
    eddm_order = settings.EDDMOrder()
    eddm_order.set_mailing_residential(True)
    eddm_order.set_touch_1_maildate(fle[-18:-4])
    eddm_order.set_touch_2_maildate()

    # get number of touches in the file
    with open(os.path.join(gblv.downloaded_orders_path, fle), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        running_cnt = 0
        for rec in csvr:
            eddm_order.file_touches = int(rec['NumberOfTouches'])
            running_cnt += int(rec['Quantity'])

        eddm_order.file_qty = running_cnt

    get_order_by_date.update_processing_file_table(fle, eddm_order, gblv)
    match_search = get_order_by_date.file_to_order_match(fle, gblv, 120)

    if match_search[0]:
        # Full successfull match, all counts match, match to downloaded order data
        print("Full Match: {}".format(fle))
        # print(match_search[1])

        process_path = os.path.join(gblv.downloaded_orders_path, match_search[1][2])

        create_process_order_path(process_path)
        # Copy original file into new directory, in 'original' folder
        copy_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1])
        # Delete the original file from the download orders path
        os.remove(os.path.join(gblv.downloaded_orders_path, fle))

    else:
        # No match to file in downloaded order data
        # TODO Move files and log errors
        pass


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


def write_ini(eddm_order, fle, mailing_date):

    configfile = os.path.join(gblv.accuzip_path, 'mail_dates.ini')

    if not os.path.exists(configfile):
        config = configparser.ConfigParser()
        config.add_section('mailing_dates')
        with open(configfile, 'w') as c:
            config.write(c)

    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(configfile)

    config.set('mailing_dates', fle, datetime.datetime.strftime(mailing_date, "%m/%d/%Y"))

    with open(configfile, 'w') as c:
        config.write(c)


def import_userdata(gblv):
    """
    Imports V2FBLUSERDATA.txt file from path, used to determine if user is still active
    """
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql = "DROP TABLE IF EXISTS `v2fbluserdata`;"
    cursor.execute(sql)

    sql = ("CREATE TABLE `v2fbluserdata` ("
           "`agent_id` VARCHAR(10) NOT NULL,"
           "`nickname` VARCHAR(60) DEFAULT NULL,"
           "`fname` VARCHAR(60) DEFAULT NULL,"
           "`lname` VARCHAR(60) DEFAULT NULL,"
           "`cancel_date` DATETIME NULL DEFAULT NULL,"
           "PRIMARY KEY (`agent_id`));")
    cursor.execute(sql)

    with open(gblv.user_data_path, 'r') as users:
        for n, line in enumerate(users):
            agentid = line[2:7]
            nickname = line[33:93].strip()
            fname = line[93:153].strip()
            lname = line[213:273].strip()
            cancel_date = (datetime.datetime.strptime(line[386:394], '%Y%m%d')
                           if not line[386:394] == '00000000' else None)

            sql = ("INSERT INTO `v2fbluserdata` VALUES (?,?,?,?,?);")
            cursor.execute(sql, (agentid, nickname, fname, lname, cancel_date,))

    conn.commit()


def download_web_orders(gblv, back_days):

    # year = 2019
    #
    # month_start = 7
    # day_start = 1
    #
    # month_end = 7
    # day_end = 8
    #
    # date_start = (datetime.datetime.strptime("{y}-{m}-{d} 00:00:00".format(
    #               m=month_start,y=year,d=str(day_start).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    # date_end = (datetime.datetime.strptime("{y}-{m}-{d} 23:59:59".format(
    #               m=month_end,y=year,d=str(day_end).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    date_end = datetime.datetime.today()
    date_start = date_end - datetime.timedelta(days=back_days)

    get_order_by_date.order_request_by_date(date_start, date_end, gblv, gblv.token)
    get_order_by_date.clean_unused_orders(gblv, gblv.token)


def create_process_order_path(process_path):
    # Creates this folder structure for the file
    if os.path.exists(process_path):
        shutil.rmtree(process_path)
        os.mkdir(process_path)
    else:
        os.mkdir(process_path)

def copy_file_to_new_folder(from_path, to_path, fle, overwrite=True):
    # Creates this folder structure for the file, deletes old structure by default
    if overwrite:
        if os.path.exists(to_path):
            shutil.rmtree(to_path)
            os.mkdir(to_path)
        else:
            os.mkdir(to_path)
    else:
        os.mkdir(to_path)

    shutil.copy2(os.path.join(from_path, fle),
                 os.path.join(to_path, fle))


def process_order(file):
    process_path = os.path.join(gblv.downloaded_orders_path, file[:-4])
    create_process_order_path(process_path)
    process_dat(file)


if __name__ == '__main__':
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    gblv.set_environment('QA')
    # gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.set_token_name()
    gblv.set_db_name()

    # if not os.path.exists(os.path.join(os.curdir, gblv.db_name)):
    #     get_order_by_date.intialize_databases(gblv)

    get_order_by_date.initialise_databases(gblv)
    get_order_by_date.processing_files_table(gblv)
    # import_userdata(gblv)
    # download_web_orders(gblv, 15)

    # exit()
    # TODO add code here for running through hold path for orders

    orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']
    for order in orders:
        process_dat(order)
        # os.remove(os.path.join(gblv.downloaded_orders_path, order))
