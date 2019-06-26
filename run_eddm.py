import dbfread
import dbf
import csv
import requests
import xml.etree.ElementTree as ET
import os
import math


class GlobalVar():
    def __init__(self):
        self.mail_residential = False

    def set_mailing_residential(self, val):
        self.mail_residential = bool(val)

    def record_addressee(self, route):
        if route[0] == 'B':
            return 'PO BOX HOLDER'
        elif self.mail_residential == True:
            return 'RESIDENTIAL CUSTOMER'
        return 'POSTAL CUSTOMER'


def usps_zip_lookup(zipcode):
    url = 'https://secure.shippingapis.com/ShippingAPI.dll?API=CityStateLookup&XML={0}'
    usps_userid = '813BINDE5230'

    if isinstance(zipcode, (list, tuple, set)):
        shell = '<CityStateLookupRequest USERID="{userid}">{r}</CityStateLookupRequest>'
        r = ''
        for n, rec in enumerate(zipcode):
            r += ('<ZipCode ID="{n}"><Zip5>{zipcode}</Zip5>'
                  '</ZipCode>'.format(n=n, zipcode=rec))
        r = shell.format(userid=usps_userid, r=r)

    else:
        r = ('<CityStateLookupRequest USERID="{userid}">'
             '<ZipCode ID="0"><Zip5>{zipcode}</Zip5>'
             '</ZipCode></CityStateLookupRequest>'.format(userid=usps_userid, zipcode=zipcode))

    response = requests.get(url.format(r))
    tree = ET.fromstring(response.content)

    request_d = dict()
    for branch in tree:
        response_d = dict()
        for child in branch:
            response_d[child.tag] = child.text
        request_d[response_d['Zip5']] = response_d

    return request_d


def write_dbf(count_file, city_dic, header):
    newdb = os.path.splitext(os.path.basename(count_file))[0]

    db = dbf.Table("{0}".format(newdb), ('FIRST C(25); ADDRESS C(1); CITY C(28); '
                                         'ST C(2); ZIP C(10); CRRT C(4); '
                                         'WALKSEQ_ C(7); STATUS_ C(1); '
                                         'BARCODE C(14); X C(1)')
                   )

    db_counts = dbf.Table("{0} CRRT Counts".format(newdb), ('ZIP C(6); CRRT C(5); RES C(6); POS C(5)'))

    db.open(mode=dbf.READ_WRITE)
    db_counts.open(mode=dbf.READ_WRITE)


    with open(count_file, 'r') as routes:
        csvr = csv.DictReader(routes, header)
        next(csvr)
        for rec in csvr:
            formatted_crrt = "{0}{1}".format((rec['crrt'][0]).upper(), str(rec['crrt'][1:]).zfill(3))
            addressee = gblv.record_addressee(formatted_crrt)
            repeats = int(rec['cnt'])
            db_counts.append((rec['zip'], formatted_crrt, str(rec['cnt']).zfill(5), str(rec['pos']).zfill(5)),)

            for n in range(0, repeats):
                # print(rec['zip'], formatted_crrt, n, repeats)

                db.append((addressee, '',
                           city_dic[rec['zip']]['City'],
                           city_dic[rec['zip']]['State'],
                           city_dic[rec['zip']]['Zip5'],
                           formatted_crrt, '0000001', 'N',
                           '/{zip}{ckd}/'.format(zip=city_dic[rec['zip']]['Zip5'],
                                                 ckd=zip_ckd(city_dic[rec['zip']]['Zip5'])),
                           ''),)
                # print(city_dic[rec['zip']])
                # if n > 0: break

    db.close()
    db_counts.close()


def chunks(lst, n):
    lst = list(lst)
    # For item i in a range that is a length of lst,
    for i in range(0, len(lst), n):
        # Create an index range for lst of n items:
        yield lst[i: i + n]


def read_input(fle):

    header = ["zip", "crrt", "cnt", "pos"]
    zips = set()

    with open(fle, 'r') as count_file:
        csvr = csv.DictReader(count_file, header)
        next(csvr)
        for rec in csvr:
            zips.add(rec['zip'])

    city_st_zip_dict = dict()

    for chunk in chunks(zips, 5):
        city_st_zip_dict.update(usps_zip_lookup(chunk))

    write_dbf(fle, city_st_zip_dict, header)


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


def csv_from_counts_file(fle):
    db = dbfread.DBF('{fle}.dbf'.format(fle=fle))
    header = ["zip", "crrt", "cnt", "pos"]
    # print(db.fields)

    newcsv = os.path.splitext(os.path.basename(fle))[0]

    with open("{0}.csv".format(newcsv), 'w+', newline='') as s:
        csvw = csv.DictWriter(s, header, delimiter=',', quoting=csv.QUOTE_ALL)
        csvw.writeheader()

        for rec in db:
            csvw.writerow({'zip': rec['zip'],
                           'crrt': rec['crrt'],
                           'cnt': rec['cnt'],
                           'pos': rec['pos']})


if __name__ == '__main__':
    # TODO Script to pick residential or simplified all routes
    global gblv
    gblv = GlobalVar()
    gblv.set_mailing_residential(False)
    # csv_from_counts_file('SIMPLIFIED ADR 50309,50312 CRRT Counts')
    # read_dbf('SIMPLIFIED ADR 50309,50312 CRRT Counts')

    # read_input('crrt counts.csv')
    # read_dbf('crrt counts')

    # read_input('acculist.csv')
    # read_dbf('acculist')

    read_input('usps_eddm.csv')
    read_dbf('usps_eddm')
