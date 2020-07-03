"""
Created on Feb 28th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
import re
import datetime


def office_scraper(city, page):
    search_base = 'https://{}.diandianzu.com'.format(city)
    search_url = 'http://cmispub.cicpa.org.cn/cicpa2_web/OfficeIndexAction.do'
    search_response = requests.post(search_url, data={'pageSize': '15', 'pageNum': '1', 'method': 'indexQuery', 'queryType': '1', 'isStock': '00', 'ascGuid': '0000010F84968569DDB2CD9ADD2CAA43', 'offName': '', 'offAllcode':'', 'personNum': '10'})
    search_soup = BeautifulSoup(search_response.text, 'lxml')
    print(search_soup)
    search_result_page = pd.DataFrame()

    property_list = search_soup.find_all('a', attrs={'class': 'tj-pc-listingList-title-click'})

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
        property_name = property.text
        property_url = search_base + property_link
        property_response = requests.get(property_url)
        property_soup = BeautifulSoup(property_response.text, 'lxml')

        office_title = property_soup.find('div', attrs={'class': 'ftitle clearfix'})
        if not office_title:
            office = dict()
            office['写字楼'] = property_name
            search_result_page = search_result_page.append(office, ignore_index=True, sort=False)
            continue
        office_title = office_title.find_all('div')[1:]
        office_list = property_soup.find('div', attrs={'class': 'fbody'}).find_all('div', attrs={'class': re.compile('fitem .*')})

        print(property_name, len(office_list))
        for row in office_list:

            office = dict()

            col = row.find('div').find_next_sibling()

            for title in office_title:
                office[title.text.strip()] = col.text.strip()
                col = col.find_next_sibling()

            if '单价 · 总价' in office.keys():

                office['单价'] = office['单价 · 总价'].split()[0].strip()
                office['总价'] = office['单价 · 总价'].split()[1].strip()
                del office['单价 · 总价']

            office['Office_ID'] = row['data-id']
            office['Property'] = property_name
            office['Property_ID'] = property_id
            search_result_page = search_result_page.append(office, ignore_index=True, sort=False)
    print('Page {} Done.'.format(str(page)))
    return search_result_page


if __name__ == '__main__':
    site = 'Diandianzu'
    date = str(datetime.date.today())
    cities = ['gz']
    # scrapydb = db.Mssql(keys.dbconfig)

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
        # scrapydb.upload(onecity, 'Scrapy_{}'.format(site), timestamp=date, source=site, city=city)
        # scrapydb.close()