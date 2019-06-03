# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import keys
import db
import pagemanipulate as pm
import utility_email as em
from utility_commons import *

SITE = 'FirePublic'
TABLENAME = 'Scrapy_FirePublic'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)


class FirePublic:

    # Update __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION before search
    def __init__(self):
        self.searchbase = 'http://210.76.69.38:82/JDGG/QTCGList.aspx?CGLX=A1'
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        self.form_data = {
            '__EVENTTARGET': 'ctl00$MainContent$AspNetPager1',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': '',
            'ctl00$MainContent$txtKeyValue:': '',
            'ctl00$MainContent$dropGJZ': '1',
            'ctl00$MainContent$txtBTime': '',
            'ctl00$MainContent$txtETime': '',
            'ctl00$MainContent$dropState': '1',
            'ctl00$MainContent$dropShi': '#',
            'ctl00$MainContent$dropQu': '#',
            'ctl00$MainContent$AspNetPager1_input': '1',
            '__VIEWSTATEGENERATOR': '58F1D55E',
            '__EVENTVALIDATION': ''
        }
        self.switch = False
        self.compSession = requests.Session()

    # Update request input
    """
    __EVENTARGUMENT: page
    txtKeyValue: keyword
    dropGJZ: keyword flag
    dropState: pass result
    """
    def update_form_data(self, **kwargs):
        for key, value in kwargs.items():
            if (key in self.form_data.keys()) or (('ctl00$MainContent$' + key) in self.form_data.keys()):
                self.form_data[key] = value

    # Do one search
    def search(self):
        response = self.compSession.post(self.searchbase, data=self.form_data)
        page_soup = BeautifulSoup(response.text, 'html.parser')
        return page_soup

    # Renew session with form data and cookies
    def renew_session(self):
        logger.info('Renew cookies')
        with pm.Page(self.searchbase) as page:
            renew_soup = BeautifulSoup(page.driver.page_source, 'lxml')
            renew_form = renew_soup.find_all('input', attrs={'id': list(self.form_data.keys())})
            for form in renew_form:
                if form['value'] != '':
                    self.form_data[form['id']] = form['value']
            self.compSession = requests.Session()
            renew_cookies = page.get_requests_cookies()
            self.compSession.cookies.update(renew_cookies)
            self.switch = True

    @classmethod
    def run(cls, from_page=1, to_page=0, keyword=None):
        df = pd.DataFrame()
        fp = cls()
        fp.renew_session()
        if keyword is not None:
            fp.update_form_data(txtKeyValue=keyword)

        # Get column names and total page
        first_soup = fp.search()
        reg_num = re.compile(r'\d+')
        total_page = reg_num.findall(first_soup.find('div', attrs={'id': 'ctl00_MainContent_AspNetPager1'}).find('td').text)
        if total_page:
            logger.info('Total {} records, {} pages.'.format(total_page[0], total_page[1]))
            total_page = int(total_page[1])
            if to_page < 1:
                to_page = total_page + 1
        colnames = first_soup.find('tr', attrs={'class': 'Grid_Title'}).find_all('th')

        i = from_page
        while (from_page <= i) and (i <= to_page):

            fp.update_form_data(__EVENTARGUMENT=str(i))
            soup = fp.search()
            try:
                content = soup.find('table', attrs={'id': 'ctl00_MainContent_gridQTCG'}).find_all('td')
            # If error, renew form data. If form data has already renewed, stop search
            except Exception as e:
                if fp.switch is True:
                    logger.error('Stop run due to: {}'.format(e))
                    break
                else:
                    fp.renew_session()
                    logger.info('Restart at page {}'.format(i))
                    continue
            # If search is working after renewed, set flag as false again
            if fp.switch is True:
                fp.switch = False

            k = 0
            while k < len(content):
                row = dict()
                while True:
                    row[colnames[k % len(colnames)].text.strip()] = content[k].text.strip()
                    k += 1
                    if k % len(colnames) == 0:
                        df = df.append(row, ignore_index=True)
                        break

            logger.info('Page {} done.'.format(i))
            i += 1
        start_page = from_page
        end_page = i - 1
        if not df.empty:
            df['Source_ID'] = df['文书编号']
            df['办结时间'] = pd.to_datetime(df['办结时间'], format='%Y-%m-%d')
        return df, start_page, end_page


if __name__ == '__main__':

    with db.Mssql(keys.dbconfig) as scrapydb, em.Email() as scrapyemail:
        df, start, end = FirePublic.run(from_page=1, to_page=2)  # 3923
        if not df.empty:
            logger.info('Start from page {}, stop at page {}.'.format(start, end))
            # df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}_{}.xlsx'.format(site, date, start, end), index=False,
            #             header=True, sheet_name=site)
            scrapydb.upload(df, TABLENAME, new_id=True, dedup=True, start=str(start), end=str(end), timestamp=TIMESTAMP, source=SITE)
        else:
            logger.info('Fail this run at page {}.'.format(end))
        scrapyemail.send(SITE, 'Done', LOG_PATH)