"""
Created on Thur Feb 28th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
from pagemanipulate import Page
import re
import datetime
import db
import keys


def office_scraper(city, pagenum):
    search_base = 'http://www.gzlawyer.org'
    search_url = search_base + '/searchGzLawFirm?name=&x=12&y=13&licenseNumber=&creditCode=&page={}'.format(pagenum)
    page = Page(search_url, 'normal')
    search_soup = BeautifulSoup(page.driver.page_source, 'lxml')
    property_list = search_soup.find('div', attrs={'class': 'chengxin'}).find_all('a')
    page.close()

    search_result_page = pd.DataFrame()

    if len(property_list) <= 1:
        return False

    for property in property_list:

        time.sleep(random.randint(0, 1) / 5.0)
        print(property.text)
        property_link = property['href']
        property_url = search_base + property_link

        try:
            property_page = Page(property_url, 'normal')
        except Exception as e:
            print(e)
            continue

        property_soup = BeautifulSoup(property_page.driver.page_source, 'lxml')
        property_page.close()

        office_list = property_soup.find('table', attrs={'class': 'form-main'}).find_all('td')
        office = dict()
        i = 0
        while i < len(office_list):
            if office_list[i].text == '注册律师人员':
                i += 1
                continue
            else:
                office[office_list[i].text] = office_list[i + 1].text
                i += 2

        search_result_page = search_result_page.append(office, ignore_index=True, sort=False)
    print('Page {} Done.'.format(str(pagenum)))
    return search_result_page


if __name__ == '__main__':
    site = 'LawFirm'
    date = str(datetime.date.today())
    cities = ['gz']
    scrapydb = db.Mssql(keys.dbconfig)

    for city in cities:

        onecity = pd.DataFrame()
        page = 29
        while True:
            one_page = office_scraper(city, page)
            if one_page is False:
                break
            else:
                onecity = onecity.append(one_page, ignore_index=True, sort=False)
                page += 1
            onecity.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)

        for col in onecity:
            onecity = onecity.fillna('')
            onecity[col] = onecity[col].apply(lambda x: str(x).strip())
        onecity['专职律师人数'] = onecity['专职律师'].apply(lambda x: len(re.split(r'\s{2,}', str(x))))
        onecity['香港律师人数'] = onecity['香港律师'].apply(lambda x: len(re.split(r'\s{2,}', str(x))))
        onecity['澳门律师人数'] = onecity['澳门律师'].apply(lambda x: len(re.split(r'\s{2,}', str(x))))
        onecity['台湾律师人数'] = onecity['台湾律师'].apply(lambda x: len(re.split(r'\s{2,}', str(x))))
        onecity['兼职律师人数'] = onecity['兼职律师'].apply(lambda x: len(re.split(r'\s{2,}', str(x))))
        onecity['总律师人数'] = onecity['专职律师人数'] + onecity['香港律师人数'] + onecity['澳门律师人数'] + onecity['台湾律师人数'] + onecity['兼职律师人数']
        onecity.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)
        scrapydb.upload(onecity, 'Scrapy_{}'.format(site), timestamp=date, source=site, city=city)
        scrapydb.close()