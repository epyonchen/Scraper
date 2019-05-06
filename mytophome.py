"""
Created on Sun Jan 15th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
from pagemanipulate import pm
import re
import datetime
import db
import keys


def office_scraper(city, page):
    search_base = 'http://{}.mytophome.com'.format(city)
    search_url = search_base + '/estateList/rent/X_0_0_0_0_0_0_0_0.html?orderBy=&isAsc=D&page={}'.format(str(page))
    search_response = requests.get(search_url)
    search_soup = BeautifulSoup(search_response.text, 'lxml')
    search_result_page = pd.DataFrame()

    property_list = search_soup.find_all('div', attrs={'class': 'fyliright'})
    if len(property_list) < 1:
        return False
    for property in property_list:

        time.sleep(random.randint(0, 1) / 5.0)

        property_url = property.h3.a['href']
        property_id = re.findall(r'\d+', property_url)[0]
        property_name = property.h3.a.text
        property_location = property.find('div', attrs={'class': 'fylifont01'})
        property_area = property_location.span.text
        property_address = property_location.text.replace(property_area, '').strip()
        property_area = property_area.replace('|', '').strip()
        print(property_name)

        webpage = pm(property_url)
        tab = 1
        while webpage.click('//li[@id="RPx{}"]/a'.format(tab)):
            search_soup = BeautifulSoup(webpage.soup, 'lxml')
            office_list = search_soup.find_all('div', attrs={'class': 'fyliright'})
            for row in office_list:
                office = dict()
                office['Office'] = row.h3.a.text
                office['Office_ID'] = re.findall(r'\d+', row.h3.a['href'])[0]
                details = row.find('div', attrs={'class': 'fylifont01'}).find_all('span')
                office['物业费'] = details[0].text
                office['面积'] = details[1].text
                office['单价'] = details[2].text
                office['月租'] = row.find('div', attrs={'class': 'fylirab'}).p.span.text
                office['近90天带看'] = row.find('div', attrs={'class': 'fylirab02'}).p.text
                office['Property_ID'] = property_id
                office['property'] = property_name
                office['地区'] = property_area
                office['地址'] = property_address
                search_result_page = search_result_page.append(office, ignore_index=True, sort=False)
            tab += 1
        webpage.close()

    print('Page {} Done.'.format(str(page)))
    return search_result_page


if __name__ == '__main__':

    site = 'Mytophome'
    date = str(datetime.date.today())
    cities = ['gz']
    scrapydb = db.Mssql(keys.dbconfig)

    for city in cities:

        onecity = pd.DataFrame()
        page = 0
        while True:
            one_page = office_scraper(city, page)
            if one_page is False:
                break
            else:
                onecity = onecity.append(one_page, ignore_index=True, sort=False)
                page += 20

        onecity.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)
        scrapydb.upload(onecity, 'Scrapy_{}'.format(site), timestamp=date, source=site,  city=city)
        scrapydb.close()
