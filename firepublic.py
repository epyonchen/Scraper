# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import datetime
import keys
import db
import pagemanipulate as pm

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
            '__VIEWSTATE': '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',
            'ctl00$MainContent$txtKeyValue:': '',
            'ctl00$MainContent$dropGJZ': '1',
            'ctl00$MainContent$txtBTime': '',
            'ctl00$MainContent$txtETime': '',
            'ctl00$MainContent$dropState': '1',
            'ctl00$MainContent$dropShi': '#',
            'ctl00$MainContent$dropQu': '#',
            'ctl00$MainContent$AspNetPager1_input': '1',
            '__VIEWSTATEGENERATOR': '58F1D55E',
            '__EVENTVALIDATION': 'GVEpcI3bnGoDenydh9MX1pIb4f+qPtlCEbl6TKkTdfC3NX4m9/9BDOTV0htFV1F5ZAuaEJmydX255uLVAwxsMK11AVvLqKXQSSpZcVjJIFsELB3UGcCO037MoMPr5xUYWcrw5tsh8YWfoJTvDwOZA7w1q/lG7ZSn550iFZGgvVCdHXxGlxjJwjUXb5PDLBioXisODEfUNu9bmn14lxqem8zXZC+TOHqpULkmm/e+wR/WITFKotISyi4ZktbTJHEhYQeFxLCzmZmIzZVPu9hyHWAKO38EYsxFXHauUT2ftpLwaAg6rzzUKvHUckgq6VKebBPpeDHfRO95lJwlMsL2paz+Ggu0YpFrwfwGtIMywOSMpkzfgig/1VE369GyzalZPZCaLumfvy8CLaXRcmW8zaQdCdhIBqWVvHoCLCb95iNsEUYmVn4PejfNKz2LqGXjMc2XWyGXI7DL6xnmxelNyMydrGvCYzDwQqcq329dhTiN28kaXBQBi/VXI7koz/NsAGEJ/wXS3fsn4L+Ecc9/4sbk+RVVjOH4ZtWa/WLPaqyokSV7YvmZzQV1nIRaLaJZA29QjfJ11C28v8RNltyOlewpKxUrOEXLSqZbtNftnMC3bTYhkox74UNex+ycz9XprYbhIu2laS6Gq/roojsWXQYMxPMWbCr2kJ2MHlpV9BLDJCQq5hpzys8tDZVz8MMQB1RleAiRND5zUGesR/rkvkNQIfgROjQ+zxTG4hqcM60Rw1sCcmniNwri1Rlh8O58DzpKLnOYfdkaTXFP1yC+Nww+5IfuypkUR5thGRga5qofovcBDtTQCPoL7jtxIQvB6LwHKkB7EWD/Z3ABu1pY5i2Haj0Am9jJIGCZ4OwF4JeHkEA0ySXVOleNOkk='
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

    # Call seleium and renew form data
    def renew_form_data(self):
        page = pm.Page(self.searchbase)
        renew_soup = BeautifulSoup(page.driver.page_source, 'lxml')
        renew_form = renew_soup.find_all('input', attrs={'id': list(self.form_data.keys())})
        for form in renew_form:
            if form['value'] != '':
                self.form_data[form['id']] = form['value']
            self.compSession = requests.Session()
        page.close()
        self.switch = True

    @classmethod
    def run(cls, from_page=1, to_page=0, keyword=None):
        df = pd.DataFrame()
        fp = cls()
        if keyword is not None:
            fp.update_form_data(txtKeyValue=keyword)

        # Get column names and total page
        first_soup = fp.search()
        reg_num = re.compile(r'\d+')
        total_page = reg_num.findall(first_soup.find('div', attrs={'id': 'ctl00_MainContent_AspNetPager1'}).find('td').text)
        if total_page:
            logging.info('Total {} records, {} pages.'.format(total_page[0], total_page[1]))
            total_page = int(total_page[1])
            if to_page < 1:
                to_page = total_page + 1
        colnames = first_soup.find('tr', attrs={'class': 'Grid_Title'}).find_all('th')

        for i in range(from_page, to_page):

            fp.update_form_data(__EVENTARGUMENT=str(i))
            soup = fp.search()
            try:
                content = soup.find('table', attrs={'id': 'ctl00_MainContent_gridQTCG'}).find_all('td')
            # If error, renew form data. If form data has already renewed, stop search
            except Exception as e:
                if fp.switch is True:
                    logging.info('Stop run due to: {}'.format(e))
                    break
                else:
                    fp.renew_form_data()
                    logging.info('Restart at page {}'.format(i))
                    i -= 1
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
            # if i > 3:
            #     break
            logging.info('Page {} done.'.format(i))
        start_page = from_page
        end_page = i - 1
        if not df.empty:
            df['Source_ID'] = df['文书编号']
        return df, start_page, end_page


if __name__ == '__main__':

    df, start, end = FirePublic.run(from_page=3414, to_page=4000)
    if not df.empty:
        logging.info('Start from page {}, stop at page {}.'.format(start, end))
        site = 'FirePublic'
        date = datetime.date.today().strftime('%Y-%m-%d')
        df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}_{}.xlsx'.format(site, date, start, end), index=False,
                    header=True, sheet_name=site)
        scrapydb = db.Mssql(keys.dbconfig)
        scrapydb.upload(df, 'Scrapy_{}'.format(site), str(start), str(end), timestamp=date, source=site)
        scrapydb.close()
    else:
        logging.info('Fail this run at page {}.'.format(end))
