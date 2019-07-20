import dbf
import csv
# import requests
# import xml.etree.ElementTree as ET
import os
import math
import shutil
import datetime
import configparser
import settings
import get_order_by_date
import fpdf

"""
This script will process FB EDDM lists downloaded from eddm order portal
"""


def create_database(eddm_order, fle_path, db_name, order_file, copy_to_accuzip=True):
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

    db_counts = dbf.Table("{0} CRRT Counts".format(full_newdb_path), 'ZIP C(6); CRRT C(5); RES C(6); POS C(5)')

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

    if copy_to_accuzip:
        print_log("\tmoving to accuzip folder: {}".format(db_name))
        shutil.copy(os.path.join(fle_path, "{}.dbf".format(db_name)),
                    os.path.join(gblv.accuzip_path, "{}.dbf".format(db_name)))


def write_azzuzip_files(eddm_order, fle_path, fle, match_search, copy_to_accuzip=True):

    # If one touch, make one file
    if eddm_order.order_touches == 1:
        insert_values = {'filename': fle, 'jobname': match_search[2],
                         'processing_date': datetime.datetime.now(),
                         'order_records': eddm_order.file_qty,
                         'total_touches': eddm_order.order_touches,
                         'touch': 1, 'mailing_date': eddm_order.touch_1_maildate,
                         'user_id': match_search[8]}

        # Write eddm order db file, copy to accuzip folder
        create_database(eddm_order, fle_path, insert_values['jobname'], fle, copy_to_accuzip)
        # Create pdf tags, save to job directory
        create_job_tags(fle_path, **insert_values)
        # Insert order into FileHistory table
        get_order_by_date.update_file_history_table(gblv, **insert_values)
        # Update ini file in accuzip folder
        write_ini(insert_values['jobname'], insert_values['mailing_date'])

    # If two touches, make two files
    if eddm_order.order_touches == 2:
        for i, t in enumerate(['_1', '_2'], 1):
            insert_values = {'filename': "{0}{1}.dat".format(fle[:-4], t),
                             'jobname': "{0}_{1}".format(match_search[2], i),
                             'processing_date': datetime.datetime.now(),
                             'order_records': eddm_order.file_qty,
                             'total_touches': eddm_order.order_touches,
                             'touch': i,
                             'mailing_date': {1: eddm_order.touch_1_maildate, 2: eddm_order.touch_2_maildate}[i],
                             'user_id': match_search[8]}

            # Write eddm order db file, copy to accuzip folder
            create_database(eddm_order, fle_path, insert_values['jobname'], fle, copy_to_accuzip)
            # Create pdf tags, save to job directory
            create_job_tags(fle_path, **insert_values)
            # Insert order into FileHistory table
            get_order_by_date.update_file_history_table(gblv, **insert_values)
            # Update ini file in accuzip folder
            write_ini(insert_values['jobname'], insert_values['mailing_date'])


def process_dat(fle):
    eddm_order = settings.EDDMOrder()
    eddm_order.set_mailing_residential(True)
    eddm_order.set_touch_1_maildate(fle[-18:-4])
    eddm_order.set_touch_2_maildate(fle[-18:-4])

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
    match_search = get_order_by_date.file_to_order_hard_match(fle, gblv, 120)

    if match_search[0]:
        # Successful match, all counts match, match to downloaded order data
        print_log("Full Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        # process_path = os.path.join(gblv.downloaded_orders_path, match_search[1][2])
        process_path = os.path.join(gblv.save_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1])
        # update processing files table, set processing date
        get_order_by_date.extended_update_processing_file_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Hard match, order processed")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_hard_previous_match(fle, gblv, 120)[0]:
        # Hard match, matches to previous order
        print_log("Hard match to previous order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle,
                                                              "Hard match to previous job, moved to duplicate")
        create_directory_path(gblv.duplicate_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no duplicates folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(gblv.duplicate_orders_path),
                                fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_soft_match(fle, gblv, 120)[0]:
        # Soft match, mail counts don't match, touch count may not match
        match_search = get_order_by_date.file_to_order_soft_match(fle, gblv, 120)

        print_log("Soft Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        # process_path = os.path.join(gblv.downloaded_orders_path, match_search[1][2])
        process_path = os.path.join(gblv.hold_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1], False)
        # update processing files table, set processing date
        get_order_by_date.extended_update_processing_file_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Soft match, moved to hold")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_previous_match(fle, gblv, 120)[0]:
        # Soft match, mail counts don't match, touch count may not match, matches to previous order
        print_log("Match to previous order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Soft match to previous job, moved to hold")
        create_directory_path(gblv.duplicate_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no duplicates folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.duplicate_orders_path,
                                fle, delete_original=gblv.delete_original_files)

    else:
        # No match, move to error
        print_log("No match to Marcom order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle, "NO MATCH TO MARCOM ORDER")
        create_directory_path(gblv.no_match_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no match folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.no_match_orders_path,
                                fle, delete_original=gblv.delete_original_files)


def zip_ckd(zipcode):
    val = sum_digits(zipcode)
    return int(((math.ceil(val / 10.0)) * 10) - val)


def create_job_tags(fle_path, **val):

    if val['total_touches'] > 1:
        if val['touch'] == 2:
            pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
            pdf = fpdf.FPDF('L', 'in', 'Letter')
            pdf.add_page()
            pdf.set_margins(.25, 0, 0)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.25, val['jobname'], 0, 0, 'C')
            pdf.set_font('Arial', 'B', 70)
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'],
                                                                                   "%Y/%m/%d")), 0, 0,
                     'C')
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Touch {} of 2".format(val['touch']), 0, 0, 'C')

            pdf.set_y(pdf.get_y() + 1.25)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.25, "WAIT FOR APPROVAL".format(val['touch']), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + .65)
            pdf.cell(10.5, 1.25, "BEFORE MAILING".format(val['touch']), 0, 0, 'C')
        else:
            pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
            pdf = fpdf.FPDF('L', 'in', 'Letter')
            pdf.add_page()
            pdf.set_margins(.25, 0, 0)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.5, val['jobname'], 0, 0, 'C')
            pdf.set_font('Arial', 'B', 70)
            pdf.set_y(pdf.get_y() + 2)
            pdf.cell(10.5, 1.5, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'],
                                                                                  "%Y/%m/%d")), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + 2)
            pdf.cell(10.5, 1.5, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')

            pdf.set_y(pdf.get_y() + 1.6)
            pdf.cell(10.5, 1.25, "Touch {} of 2".format(val['touch']), 0, 0, 'C')

    else:
        pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
        pdf = fpdf.FPDF('L', 'in', 'Letter')
        pdf.add_page()
        pdf.set_margins(.25, 0, 0)
        pdf.set_font('Arial', 'B', 70)
        pdf.cell(10.5, 1.5, val['jobname'], 0, 0, 'C')
        pdf.set_y(pdf.get_y() + 2)
        pdf.cell(10.5, 1.5, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'], "%Y/%m/%d")), 0, 0,
                 'C')
        pdf.set_y(pdf.get_y() + 2)
        pdf.cell(10.5, 1.5, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')

    pdf.output(os.path.join(fle_path, pdf_name), 'F')


def sum_digits(n):
    if not isinstance(n, (int,)):
        n = int(n)

    r = 0
    while n:
        r, n = r + n % 10, n // 10
    return r


def print_log(message):
    """
    Sends message to console, saves message to message log to be recalled later
    :param message: text message to console
    :return:
    """
    print(message)
    gblv.log_messages.append(message)


def write_ini(fle, mailing_date):

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


def download_web_orders(back_days):

    # year = 2019
    # month_start = 7
    # day_start = 1
    # month_end = 7
    # day_end = 8
    # date_start = (datetime.datetime.strptime("{y}-{m}-{d} 00:00:00".format(
    #               m=month_start,y=year,d=str(day_start).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    # date_end = (datetime.datetime.strptime("{y}-{m}-{d} 23:59:59".format(
    #               m=month_end,y=year,d=str(day_end).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    date_end = datetime.datetime.today()
    date_start = date_end - datetime.timedelta(days=back_days)

    get_order_by_date.order_request_by_date(date_start, date_end, gblv, gblv.token)
    get_order_by_date.clean_unused_orders(gblv, gblv.token)


def create_directory_path(process_path, overwrite=False):
    # Creates this folder structure for the file
    if overwrite:
        if os.path.exists(process_path):
            shutil.rmtree(process_path)
            os.makedirs(process_path)
        else:
            os.makedirs(process_path)
    else:
        if not os.path.exists(process_path):
            os.makedirs(process_path)


def move_file_to_new_folder(from_path, to_path, fle, overwrite=False, delete_original=False):
    # Creates this folder structure for the file, does not overwrite by default
    # Moves fle from from_path to to_path, does not delete old file by default
    if overwrite:
        if os.path.exists(to_path):
            shutil.rmtree(to_path)
            os.mkdir(to_path)
        else:
            os.mkdir(to_path)
    else:
        if not os.path.exists(to_path):
            os.mkdir(to_path)

    shutil.copy2(os.path.join(from_path, fle),
                 os.path.join(to_path, fle))

    if delete_original:
        os.remove(os.path.join(from_path, fle))


def process_order(file):
    process_path = os.path.join(gblv.downloaded_orders_path, file[:-4])
    create_directory_path(process_path)
    process_dat(file)


def date_ordered_file_list():
    current_orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']
    dic = {}
    for order in current_orders:
        dic[order[:-4].split("_")[1]] = order

    sorted_dic = sorted(dic.items(), key=lambda kv: datetime.datetime.strptime(kv[0], "%Y%m%d%H%M%S"), reverse=True)

    return [v for k, v in sorted_dic]


def write_tag_merge():
    tag_filename = "TAG_MERGE_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%I %M %p"))

    with open(os.path.join(gblv.downloaded_orders_path, tag_filename), 'w+') as log:
        log.write("jobname\tmailing date\tfilecount\n")
        for line in get_order_by_date.processing_files_log(gblv):
            log.write("{0}\t{1}\t{2:,}\n".format(line[1], line[8], line[3]))


def job_agent_status(days):
    report_filename = "Agent_Job Status_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))

    date_from = datetime.datetime.strftime(datetime.datetime.today(), "%Y/%m/%d")
    date_to = datetime.datetime.strftime(datetime.timedelta(days=(days - 1)) + datetime.datetime.today(), "%Y/%m/%d")

    with open(os.path.join(gblv.downloaded_orders_path, report_filename), 'w+') as log:
        log.write("Agent status for jobs mailing {} - {}\n\n".format(date_from, date_to))

        log.write("{:<25}{:<18}{:<12}{:<12}{:<40}\n".format("Job Name",
                                                            "Mailing Date",
                                                            "Agent ID",
                                                            "Status",
                                                            "Agent Name"))

        for line in get_order_by_date.jobs_mailing_agent_status(gblv, days):
            log.write("{:<25}{:<18}{:<12}{:<12}{:<40}\n".format(line[0], line[1], line[2], line[4], line[5]))


def write_message_log():

    log_filename = "LOG_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%I %M %p"))

    with open(os.path.join(gblv.downloaded_orders_path, log_filename), 'w+') as log:
        for message in gblv.log_messages:
            log.write("{}\n".format(message))

        log.write("\n\n")
        log.write("{:<28}{:<24}{:<24}{:<14}{:>10}{:>15}{:>15}"
                  "{:>17}{:>5}{:<100}\n".format("filename",
                                                "jobname",
                                                "order date",
                                                "mailing date",
                                                "file count",
                                                "file touches",
                                                "marcom count",
                                                "marcom touches",
                                                "",
                                                "status"))

        for line in get_order_by_date.processing_files_log(gblv):
            log.write("{:<28}{:<24}{:<24}{:<14}{:>10,}{:>15,}"
                      "{:>15,}{:>17,}{:>5}{:<100}\n".format(line[0],
                                                            line[1],
                                                            line[2],
                                                            line[8],
                                                            line[3],
                                                            line[4],
                                                            line[5],
                                                            line[6],
                                                            "",
                                                            line[7]))


if __name__ == '__main__':
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.set_token_name()
    gblv.set_db_name()

    get_order_by_date.initialize_databases(gblv)
    get_order_by_date.import_userdata(gblv)
    download_web_orders(8)

    # TODO add code here for running through hold path/no match orders for orders

    # get_order_by_date.clear_file_history_table(gblv)

    # Create a list of orders
    orders = date_ordered_file_list()
    # Create table of orders to process
    get_order_by_date.processing_files_table(gblv, orders)
    for order in orders:
        process_dat(order)

    if len(orders):
        get_order_by_date.append_filename_to_orderdetail(gblv)
        get_order_by_date.processing_table_to_history(gblv)
        write_message_log()
        # write_tag_merge()
    else:
        print("No files to process")
