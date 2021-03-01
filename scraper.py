import json
import requests
import datetime
from bs4 import BeautifulSoup as soup
from twilio.rest import Client

base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'


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
    response = requests.get(url)
    print(response.status_code)
    if response.status_code != 200:
        print('No url found.')
        return None

    decode_html = soup(response.text, 'html.parser')
    href = 'https://www.sec.gov/' + \
           decode_html.find('table', {'class': 'tableFile'}).findAll('tr')[1].findAll('td')[2].a['href']
    return href


def parse_master_idx(url):
    response = requests.get(url)
    print(response.status_code)
    if response.status_code != 200:
        print('No record found for this day.')
        return None

    daily_dict = {}
    for x in response.text.split('\n'):
        if '.txt' in x:
            pipe_split = x.split('|')
            daily_dict[pipe_split[0]] = {}
            daily_dict[pipe_split[0]]['form_type'] = pipe_split[2]
            # daily_dict[pipe_split[0]]['date_filed'] = datetime.datetime.strptime(
            #     pipe_split[3][0:4] + ' ' + pipe_split[3][4:6] + ' ' + pipe_split[3][6:8], '%Y %m %d')
            daily_dict[pipe_split[0]]['date_filed'] = pipe_split[3][0:4] + '-' + pipe_split[3][4:6] + '-' + pipe_split[
                                                                                                                3][6:8]
            daily_dict[pipe_split[0]]['link'] = ['https://www.sec.gov/Archives/' + pipe_split[4].split('.')[
                0] + '-index.html']
            href = doc_url(daily_dict[pipe_split[0]]['link'][0])
            daily_dict[pipe_split[0]]['link'].append(href)

            print(json.loads(json.dumps(daily_dict[pipe_split[0]])))
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
        elif date.month <= 12:
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
    auth_token = 'ffd5b80938868db4169769269d3c9405'
    client = Client(account_sid, auth_token)
    client.messages.create(
        from_='whatsapp:+14155238886',
        body='Run complete.',
        to='whatsapp:+919314964063'
    )


main()
