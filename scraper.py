import concurrent.futures
import datetime
import os

import requests
from bs4 import BeautifulSoup as soup
from peewee import chunked
from ratemate import RateLimit
from models import EdgarEntry, db, create_tbls, CIK

base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
rate_limit = RateLimit(max_count=10, per=1)
# keeping CIK to ticker mapping in memory as well for fast access, this is supposed to be refreshed if the user
# requested a ticker for which an entry in CIK table does not exist, we get CIK-Ticker mapping again from SEC and
# refresh this map in load_cik_ticker
ticker_to_cik = {}
cik_to_ticker = {}


class Entry:
    def __init__(self, cik=0, form_type='', date='', html_link='', doc_link=''):
        self.cik = cik
        self.form_type = form_type
        self.date = date
        self.html_link = html_link
        self.doc_link = doc_link


def load_cik_ticker():
    print('Loading ticker and CIK mappings in the database...')
    ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
    response = requests.get(ticker_cik_map_link)
    decoded_response = response.text
    pks = []  # primary keys
    rows = []
    counter = 0
    for x in decoded_response.split('\n'):
        tkr = x.split('\t')[0]
        cik = x.split('\t')[1]
        ticker_to_cik[tkr] = [cik]
        cik_to_ticker[cik] = [tkr]
        rows.append(CIK(ticker=tkr, cik=cik))

    print(len(rows), 'ticker to CIK mappings found on', ticker_cik_map_link)
    for cik in cik_to_ticker:
        pk = CIK.insert(ticker=cik_to_ticker[cik][0], cik=cik).on_conflict_replace().execute()
        ticker_to_cik[cik_to_ticker[cik][0]].append(pk)
        cik_to_ticker[cik].append(pk)

        counter += 1
        if counter % 500 == 0:
            print('{} tiker and CIK loaded'.format(counter))
    print('ticker and CIK mapping load COMPLETE')
    # print('db is assigned to %r' % db)
    # with db.atomic:
    #     CIK.bulk_create(rows, batch_size=500)


def insert_new_cik(ticker):
    ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
    response = requests.get(ticker_cik_map_link)
    decoded_response = response.text
    for x in decoded_response.split('\n'):
        tkr = x.split('\t')[0]
        cik = x.split('\t')[1]
        if ticker is not None and ticker == tkr:
            CIK.insert(ticker=tkr, cik=cik).on_conflict_ignore().execute()
            return cik

    return None


def cik_exists(ticker):
    cik = CIK.select().where(ticker=ticker)
    if cik is None or cik.cik is None:
        return insert_new_cik(ticker)
    return cik


def add_doc_link_to_obj(obj):
    rate_limit.wait()
    response = requests.get(obj.html_link)
    # response = requests.get(url, proxies=get_proxy())
    if response.status_code != 200:
        print(f'Filing link not available - {obj.html_link}')
        return None

    decode_html = soup(response.text, 'html.parser')
    obj.doc_link = 'https://www.sec.gov/' + \
                   decode_html.find('table', {'class': 'tableFile'}).findAll('tr')[1].findAll('td')[2].a['href']
    return obj


def get_edgar_daily_objects(url):
    response = requests.get(url)
    if response.status_code != 200:
        print('No record found for this day (possibly a weekend).')
        return None
    daily_dict = {}
    daily_objects_pre = []
    i = 0
    for x in response.text.split('\n'):
        if '.txt' in x:
            pipe_split = x.split('|')
            daily_dict[i] = {}
            obj = Entry()
            obj.cik = int(pipe_split[0])
            obj.form_type = pipe_split[2]
            obj.date_filed = pipe_split[3][0:4] + '-' + pipe_split[3][4:6] + '-' + pipe_split[3][6:8]
            obj.html_link = 'https://www.sec.gov/Archives/' + pipe_split[4].split('.')[
                0] + '-index.html'
            daily_objects_pre.append(obj)
            i = i + 1
    print('Total Filings: {}'.format(i))
    rows = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # daily_objects_post = executor.map(add_doc_link_to_obj, daily_objects_pre)
        daily_objects_post = {executor.submit(add_doc_link_to_obj, obj) for obj in daily_objects_pre}
        # with db.atomic:
        #     CIK.bulk_create(daily_objects_post, batch_size=500)
        # # CIK.bulk_create(rows, batch_size=500)

        for future in concurrent.futures.as_completed(daily_objects_post):
            entry = future.result()
            print(entry)
            try:
                rows.append({'cik': entry.cik, 'form_type': entry.form_type, 'date': entry.date,
                             'html_link': entry.html_link, 'doc_link': entry.doc_link})
            except KeyError:
                print('The following CIK was not found anymore \n', entry.__str__())
                continue
            if rows is not None and len(rows) % 100 == 0:
                print('writing {} rows to database'.format(len(rows)))
                with db.atomic():
                    EdgarEntry.insert_many(rows, fields=[EdgarEntry.cik, EdgarEntry.form_type, EdgarEntry.date,
                                                          EdgarEntry.html_link, EdgarEntry.doc_link]).execute()
                rows = []

        if len(rows) < 100:
            print('writing {} rows to database'.format(len(rows)))
            with db.atomic():
                EdgarEntry.insert_many(rows).execute()
        print('done backilling for a day')
    return daily_objects_post


def create_url(baseurl, date):
    if date.month <= 3:
        quarter = 'QTR1'
    elif date.month <= 6:
        quarter = 'QTR2'
    elif date.month <= 9:
        quarter = 'QTR3'
    else:
        quarter = 'QTR4'
    url = baseurl + str(date.year) + '/' + quarter + '/' + 'master.' + date.strftime('%Y%m%d') + '.idx'
    return url


def crawl_url(baseurl, start_date, end_date):
    date = start_date
    all_entries = []
    while date <= end_date:
        url = create_url(baseurl, date)
        print('Parsing data from {}'.format(url))
        all_entries.append(get_edgar_daily_objects(url))
        date = date + datetime.timedelta(days=1)
    return all_entries


# when you're ready to start querying, remember to connect
def main():
    load_cik_ticker()
    crawl_url(base_url, datetime.datetime(2021, 1, 1),
              datetime.datetime(2021, 1, 31))


db.connect()
create_tbls()
main()
if not db.is_closed():
    db.close()
