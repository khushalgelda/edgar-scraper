from ratemate import RateLimit
import requests
import datetime
from bs4 import BeautifulSoup as soup
from twilio.rest import Client
import concurrent.futures

base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
rate_limit = RateLimit(max_count=10, per=1)


class Entry:
    def __init__(self, cik, form_type, date, link):
        self.cik = cik
        self.form_type = form_type
        self.date = date
        self.link = link


def cik_for(ticker):
    ticker_cik_map_link = 'https://www.sec.gov/include/ticker.txt'
    response = requests.get(ticker_cik_map_link)
    decoded_response = response.text
    ticket_cik = {}
    for x in decoded_response.split('\n'):
        ticket_cik[x.split('\t')[0]] = x.split('\t')[1]
    return ticket_cik[ticker]


# print(cik_for('ibm'))
def doc_url(url):
    rate_limit.wait()
    response = requests.get(url)
    # response = requests.get(url, proxies=get_proxy())
    if response.status_code != 200:
        print(f'Filing link not available - {url}')
        return None

    decode_html = soup(response.text, 'html.parser')
    href = 'https://www.sec.gov/' + \
           decode_html.find('table', {'class': 'tableFile'}).findAll('tr')[1].findAll('td')[2].a['href']
    return href


def parse_master_idx(url):
    response = requests.get(url)
    if response.status_code != 200:
        print('No record found for this day.')
        return None
    links_to_request = []
    daily_dict = {}
    i = 0
    for x in response.text.split('\n'):
        if '.txt' in x:
            pipe_split = x.split('|')
            daily_dict[i] = {}
            daily_dict[i]['cik'] = pipe_split[0]
            daily_dict[i]['form_type'] = pipe_split[2]
            daily_dict[i]['date_filed'] = pipe_split[3][0:4] + '-' + pipe_split[3][4:6] + '-' + pipe_split[3][6:8]
            html_link = 'https://www.sec.gov/Archives/' + pipe_split[4].split('.')[
                0] + '-index.html'
            links_to_request.append(html_link)
            # if i == 5:
            #     break
            i = i + 1
    print('Total Filings: {}'.format(i))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        html_extracted_links = executor.map(doc_url, list(links_to_request))
    i = 0
    for link in html_extracted_links:
        daily_dict[i]['link'] = link
        i = i + 1
    # print(json.loads(json.dumps(daily_dict[pipe_split[0]])))

    return daily_dict


def crawl_url(baseurl, start_date, end_date):
    date = start_date
    all_entries = []
    while date <= end_date:
        if date.month <= 3:
            quarter = 'QTR1'
        elif date.month <= 6:
            quarter = 'QTR2'
        elif date.month <= 9:
            quarter = 'QTR3'
        else:
            quarter = 'QTR4'
        url = baseurl + str(date.year) + '/' + quarter + '/' + 'master.' + date.strftime('%Y%m%d') + '.idx'
        print('Parsing data from {}'.format(url))
        all_entries.append(parse_master_idx(url))
        date = date + datetime.timedelta(days=1)
    return all_entries


def main():
    crawl_url(base_url, datetime.datetime(2021, 2, 22),
              datetime.datetime(2021, 2, 22))
    account_sid = 'AC7cdfca43c5f8cb4a21201224954179c7'
    auth_token = '313ed88a692f37dfc434de5a136e1525'
    client = Client(account_sid, auth_token)
    client.messages.create(
        from_='whatsapp:+14155238886',
        body=f'Edgar scraper run completed.',
        to='whatsapp:+917411873829'
    )


main()
