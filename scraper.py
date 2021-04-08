import concurrent.futures
import datetime
import json
import os

import peewee
import requests
import yaml
from bs4 import BeautifulSoup as soup
from playhouse.db_url import connect
from ratemate import RateLimit

database_proxy = peewee.DatabaseProxy()


class Database:
    def __init__(self, db, user, passwd, max_connections=32, stale_timeout=32):
        self.db = db
        self.user = user
        self.passwd = passwd
        self.max_connection = max_connections
        self.stale_timeout = stale_timeout
        database_proxy.initialize(self.get_db())

    def get_db(self):
        return connect(os.environ.get('DATABASE') or 'mysql://root:root@0.tcp.ngrok.io:13604/mydb')


class BaseModel(peewee.Model):
    """A base model that will use our MySQL database"""
    created = peewee.DateTimeField(default=datetime.datetime.now())

    class Meta:
        database = database_proxy


class CIK(BaseModel):
    cik = peewee.IntegerField()
    ticker = peewee.CharField(max_length=15)
    indexes = (
        (('cik', 'ticker'), True),
    )

    def __enter__(self):
        return self


class EdgarEntry(BaseModel):
    cik = peewee.ForeignKeyField(CIK, backref='entries')
    form_type = peewee.CharField(max_length=10)
    date = peewee.DateTimeField()
    html_link = peewee.CharField()
    doc_link = peewee.CharField()

    def __enter__(self):
        return self


# simple utility function to create tables
def create_tbls():
    with database_proxy:
        database_proxy.create_tables([CIK, EdgarEntry])


base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
rate_limit = RateLimit(max_count=10, per=1)
# keeping CIK to ticker mapping in memory as well for fast access, this is supposed to be refreshed if the user
# requested a ticker for which an entry in CIK table does not exist, we get CIK-Ticker mapping again from SEC and
# refresh this map in load_cik_ticker
ticker_to_cik = {}
cik_to_ticker = {}
cik_to_ticker_existing_records = {}
ticker_to_cik_existing_records = {}


class Entry:
    def __init__(self, cik=0, form_type='', date='', html_link='', doc_link=''):
        self.cik = cik
        self.form_type = form_type
        self.date = date
        self.html_link = html_link
        self.doc_link = doc_link


def load_cik_ticker():
    # extract existing cik to ticker mappings from db
    print('Extract existing cik to ticker mappings from CIK table in DB')
    cik_ticker_query = CIK.select()
    for record in cik_ticker_query:
        cik_to_ticker_existing_records[record.cik] = [record.ticker, record.id]
        ticker_to_cik_existing_records[record.ticker] = [record.cik, record.id]

    ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
    print('Fetching ticker and CIK mapping from', ticker_cik_map_link)
    response = requests.get(ticker_cik_map_link)
    decoded_response = response.text

    pks = []  # primary keys
    counter = 0

    print('Building a dictionary for ticker and CIK mappings')
    for x in decoded_response.split('\n'):
        tkr = x.split('\t')[0]
        cik = int(x.split('\t')[1])
        cik_create_if_not_exists(cik, tkr)
        counter += 1
    print(counter, 'ticker to CIK mappings loaded in memory for fast access')


def cik_create_if_not_exists(cik, tkr):
    if cik not in cik_to_ticker_existing_records:
        print('New CIK', cik, ' to Ticker', tkr,
              ' mapping found. Inserting to DB')
        try:
            pk = CIK.insert(ticker=tkr, cik=cik).execute()
        except Exception as e:
            print('exception occured', e, tkr, cik)
        if tkr != 'n/a':
            ticker_to_cik_existing_records[tkr] = [cik, pk]
        cik_to_ticker_existing_records[cik] = [tkr, pk]


def insert_new_cik(ticker):
    ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
    response = requests.get(ticker_cik_map_link)
    decoded_response = response.text
    for x in decoded_response.split('\n'):
        tkr = x.split('\t')[0]
        cik = int(x.split('\t')[1])
        if ticker is not None and ticker == tkr:
            CIK.insert(ticker=tkr, cik=cik).on_conflict_ignore().execute()
            return cik

    return None


def get_cik_for(ticker):
    query = CIK.select().where(ticker=ticker)
    if query is None or query.cik is None:
        return insert_new_cik(ticker)
    return query.cik


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
    return work(daily_objects_pre)


def work(daily_objects_pre):
    start = datetime.datetime.now()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        daily_objects_post = {executor.submit(add_doc_link_to_obj, obj) for obj in daily_objects_pre}

        for future in concurrent.futures.as_completed(daily_objects_post):
            if future is None or future.result() is None:
                continue
            # print('cik=', int(future.result().cik), 'form_type=', future.result().form_type, 'date=',
            #       future.result().date_filed,
            #       'doc_link=', future.result().doc_link)
            print('Fetching filings for', future.result().date_filed, '...')
            while True:
                try:
                    EdgarEntry.insert(cik=cik_to_ticker_existing_records[int(future.result().cik)][1],
                                      form_type=future.result().form_type, date=future.result().date_filed,
                                      html_link=future.result().html_link, doc_link=future.result().doc_link).execute()
                    break
                except KeyError:
                    cik_create_if_not_exists(int(future.result().cik), 'n/a')
                    try:
                        write_to_file("missing_ticker.txt", str(future.result().cik))
                    except Exception as e:
                        print(e)
    try:
        write_to_file("last_day_done.txt", future.result().date_filed)
    except Exception as e:
        print(e)

    print('Fetching completed for', future.result().date_filed)
    end = (datetime.datetime.now() - start)
    print(end, "seconds")
    print(end / 60, "minutes")
    print(end / 24, "hours")


def write_to_file(file, mssg):
    f = open(file, "a")
    f.write(mssg)
    f.write("\n")
    f.close()


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


def crawl_url(baseurl="https://www.sec.gov/Archives/edgar/daily-index/", start_date=datetime.date.today(),
              end_date=datetime.date.today()):
    date = start_date
    all_entries = []
    while date <= end_date:
        url = create_url(baseurl, date)
        print('Parsing data from {}'.format(url))
        all_entries.append(get_edgar_daily_objects(url))
        date = date + datetime.timedelta(days=1)
    return all_entries


def read_json_config(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def read_yaml_config(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


# when you're ready to start querying, remember to connect
def main():
    config = read_json_config("config.json")
    Database(
        config["database"]["db"],
        config["database"]["user"],
        config["database"]["password"])
    database_proxy.connect()
    create_tbls()
    load_cik_ticker()

    # IMPORTANT: Make sure to look into last_day_done.txt to get the new start year, start day and start month
    # last_day_done.txt is the file which tells you upto when the backfill was done before script stopped thhe last
    # time.
    # crawl_url(config["query"]["base_url"],
    #           datetime.datetime(int(config["query"]["start_year"]), int(config["query"]["start_month"]),
    #                             int(config["query"]["start_date"])),
    #           datetime.datetime(int(config["query"]["end_year"]), int(config["query"]["end_month"]),
    #                             int(config["query"]["end_date"])))
    crawl_url()
    if not database_proxy.is_closed():
        database_proxy.close()


main()
