import concurrent.futures
import datetime
import requests
from bs4 import BeautifulSoup as soup
from ratemate import RateLimit

base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
rate_limit = RateLimit(max_count=10, per=1)


class Entry:
    def __init__(self, cik=0, form_type='', date='', html_link='', doc_link=''):
        self.cik = cik
        self.form_type = form_type
        self.date = date
        self.html_link = html_link
        self.doc_link = doc_link


# def cik_for(ticker):
#     ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
#     response = requests.get(ticker_cik_map_link)
#     decoded_response = response.text
#     ticket_cik = {}
#     for x in decoded_response.split('\n'):
#         ticket_cik[x.split('\t')[0]] = x.split('\t')[1]
#     return ticket_cik[ticker]


def add_doc_url(obj):
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


def parse_master_idx(url):
    response = requests.get(url)
    if response.status_code != 200:
        print('No record found for this day.')
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
            # if i == 5:
            #     break
            i = i + 1
    print('Total Filings: {}'.format(i))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # daily_objects_post = executor.map(add_doc_url, daily_objects_pre)
        daily_objects_post = [executor.submit(add_doc_url, obj) for obj in daily_objects_pre]
        # for x in concurrent.futures.as_completed(daily_objects_post):
        #     print(x.result().cik)
    # return daily_objects_post


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
        all_entries.append(parse_master_idx(url))
        date = date + datetime.timedelta(days=1)
    return all_entries


def main():
    crawl_url(base_url, datetime.datetime(2021, 1, 1),
              datetime.datetime(2021, 1, 31))


if __name__ == '__main__':
    main()
