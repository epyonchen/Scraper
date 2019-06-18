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

    # Get item details
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

    def format_df(self):
            self.df['estateId'] = self.df['estateId'].apply(lambda x: self.city + '_' + x, axis=1)
            self.df['id'] = self.df['id'].apply(lambda x: self.city + '_' + x, axis=1)


if __name__ == '__main__':

    cities = ['gz']
    with db.Mssql(keys.dbconfig) as scrapydb:

        one_city_df, start, end = MyTopHome.run(city='gz', from_page=0, step=20)
        scrapydb.upload(one_city_df, TABLENAME, start=start, end=str(int(end) % 20), timestamp=TIMESTAMP, source=SITE, city='gz')

    scrapyemail = em.Email()
    scrapyemail.send(SITE, 'Done', LOG_PATH)
    scrapyemail.close()
