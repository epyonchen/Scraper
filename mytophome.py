"""
Created on Sun Jan 15th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import random
import time
import re
import db
import keys
import requests
import json
import utility_email as em
from utility_commons import *
from scrapers import TwoStepScraper

SITE = 'Mytophome'
TABLENAME = 'Scrapy_Mytophome'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)
_data = {
    'cityId': '20',
    'crmDicId': '',
    'saleType': 'R',
    'buildArea': '0-50000',
    'page': '1',
    'pagesize': '1000'
}

class MyTopHome(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'http://{}.mytophome.com'.format(city)
        self.search_url = '/estateList/rent/X_0_0_0_0_0_0_0_0.html?orderBy=&isAsc=D&page={}'


    # Get items in one page
    def get_item_list(self, cityname, pagenum):
        list_link = self.search_url.format(pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('div', attrs={'class': 'fyliright'})
        return item_list

    def get_item_detail(self, item):

        time.sleep(random.randint(0, 1) / 5.0)
        item_link = item.h3.a['href']
        try:
            global _data
            _data['crmDicId'] = re.compile(r'\d+').search(item_link).group(0)
        except Exception as e:
            logger.exception(e)
            return False
        item_name = item.h3.a.text
        logger.info('Building Name: {}'.format(item_name))
        item_detail_list = []
        item_details = requests.post('http://api.mytophome.com/officeService/getOfficeByEstate.do', data=_data)
        item_details = json.loads(item_details.text[1:-1])
        if item_details[0]['statusCode'] == 1:
            item_detail_list += item_details[0]['estatePropList']

        return item_detail_list


        # browser.get(item_link)
        # tab_soup = BeautifulSoup(browser.soup, 'lxml')
        # tabs = len(tab_soup.find_all('li', attrs={'id': re.compile('RPx\d+')}))
        #
        # for tab in range(1, tabs):
        #     browser.click('//li[@id="RPx{}"]/a'.format(tab))
        #     tab_soup = BeautifulSoup(browser.soup, 'lxml')
        #     detail_list = tab_soup.find_all('div', attrs={'class': 'fyliright'})
        #
        #     for row in detail_list:
        #         item_detail = dict()
        #         try:
        #             item_detail['Source_ID'] = self.city + '_' + re.compile(r'\d+').search(row.h3.a['href']).group(0)
        #             infos = row.find('div', attrs={'class': 'fylifont01'}).find_all('span')
        #             item_detail['物业费'] = infos[0].text
        #             item_detail['面积(元/平方米/月)'] = infos[1].text
        #             item_detail['租金(元/平方米/月)'] = infos[2].text
        #             item_detail['近90天带看'] = row.find('div', attrs={'class': 'fylirab02'}).p.text
        #         except Exception as e:
        #             logger.exception(e)
        #             # item_detail_list.append(item_detail)
        #             continue
        #         item_detail['Property_ID'] = item_id
        #         item_detail['Property_Name'] = item_name
        #         item_detail['Name'] = row.h3.a.text
        #         item_detail_list.append(item_detail)
        #     tab += 1
        #     if not browser.exist('//li[@id="RPx{}"]/a'.format(tab)):
        #         break

        return item_detail_list

    def format_df(self):
            self.df['estateId'] = self.city + '_' + self.df['estateId']
            self.df['id'] = self.city + '_' + self.df['id']


if __name__ == '__main__':

    cities = ['gz']
    with db.Mssql(keys.dbconfig) as scrapydb:

        one_city_df, start, end = MyTopHome.run(city='gz', from_page=0, step=20)
        scrapydb.upload(one_city_df, TABLENAME, start=start, end=str(int(end) % 20), timestamp=TIMESTAMP, source=SITE, city='gz')

    scrapyemail = em.Email()
    scrapyemail.send(SITE, 'Done', LOG_PATH)
    scrapyemail.close()
