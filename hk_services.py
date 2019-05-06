"""
Created on Sun Jan 9th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
import re
import datetime
import db
import keys


def office_scraper(city, page):
    search_base = 'http://www.hkservicedirectoryingd.gov.hk'
    search_url = search_base + '/companylist?page={}&ind=0&dis=1&kw='.format(str(page))
    search_response = requests.get(search_url)
    search_soup = BeautifulSoup(search_response.text, 'lxml')
    search_result_page = pd.DataFrame()
    space = re.compile(r'\s')
    property_list = search_soup.find_all('a', attrs={'target': '_blank', 'style': 'font-size:18px;'})

    if len(property_list) <= 1:
        return False

    for property in property_list:

        time.sleep(random.randint(0, 1) / 5.0)
        property_link = property['href']
        try:
            property_id = re.compile(r'\d+').search(property_link).group(0)
        except Exception as e:
            print(e)
            continue

        property_url = search_base + property_link
        property_response = requests.get(property_url)
        property_soup = BeautifulSoup(property_response.text, 'lxml')

        property_detail = property_soup.find('table', attrs={'class': 'tbl_fdata', 'style': 'width:98%;'}).find_all('td')
        property_name = property_detail[0].text
        property_industry = property_soup.find('a', href=re.compile(r'/companylist\?ind=\d+')).text

        i = 2
        office = dict()
        office['Source_ID'] = property_id
        office['公司名'] = property_name
        office['行业'] = property_industry

        while i < len(property_detail) - 2:

            office[re.subn(space, '', property_detail[i].text, count=0)[0].replace(':', '')] = re.subn(space, '', property_detail[i + 1].text, count=0)[0].replace(':', '')
            i += 2

        search_result_page = search_result_page.append(office, ignore_index=True, sort=False)
    print('Page {} Done.'.format(str(page)))
    return search_result_page


if __name__ == '__main__':
    site = 'HK_Services_Company'
    date = str(datetime.date.today())
    cities = ['gz']
    scrapydb = db.Mssql(keys.dbconfig)

    for city in cities:

        onecity = pd.DataFrame()
        page = 1
        while True:
            one_page = office_scraper(city, page)
            if one_page is False:
                break
            else:
                onecity = onecity.append(one_page, ignore_index=True, sort=False)
                page += 1

        onecity.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)
        scrapydb.upload(onecity, 'Scrapy_{}'.format(site), timestamp=date, source=site, city=city)
        scrapydb.close()
