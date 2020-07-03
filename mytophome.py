"""
Created on Jan 15th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import random
import time
import re
import db
import requests
import json
import utility_email as em
from utility_commons import *
from scrapers import TwoStepScraper
import keys

SITE = 'Mytophome'
TABLENAME = 'Scrapy_Mytophome'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)
_header = {

}
_data = {
    'cityId': '20',
    'crmDicId': '',
    'saleType': 'R',
    'buildArea': '0-5000',
    'page': '1',
    'pagesize': '10000',
}

class MyTopHome(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'http://{}.mytophome.com'.format(city)
        self.search_url = '/estateList/rent/X_0_0_0_0_0_0_0_0.html?orderBy=&isAsc=D&page={}'

    # Get items in one page
    def get_item_list(self, pagenum):
        list_link = self.search_url.format(pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('div', attrs={'class': 'fyliright'})
        return item_list

    # Get item details
    def get_item_detail(self, item):

        def _format_query(data):
            if not isinstance(data, dict):
                return False
            param = '&'.join(['='.join(i) for i in data.items()])
            return param
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
        item_details = requests.get('https://api.mytophome.com/officeService/getOfficeByEstate.do?' + _format_query(_data))

        item_details = json.loads(item_details.text[1:-1])
        if item_details[0]['statusCode'] == 1:
            item_detail_list += item_details[0]['estatePropList']

        return item_detail_list

    def format_df(self):
            self.df['estateId'] = self.df['estateId'].apply(lambda x: self.city + '_' + x)
            self.df['id'] = self.df['id'].apply(lambda x: self.city + '_' + x)


if __name__ == '__main__':

    cities = ['gz']
    with db.Mssql(config=keys.dbconfig_mkt) as scrapydb:

        one_city, start, end = MyTopHome.run(city='gz', from_page=0, step=20)
        logger.info('Start from page {}, stop at page {}.'.format(start, end))
        print(one_city.df.groupby(['estateId', 'propertyName']))
        # scrapydb.upload(df=one_city.df, table_name=TABLENAME, schema='CHN_MKT', start=start, end=str(int(end) % 20), timestamp=TIMESTAMP, source=SITE, city='gz')

    scrapyemail = em.Email()
    scrapyemail.send(subject='[Scrapy] ' + TABLENAME, content='Done', attachment=LOG_PATH)
    scrapyemail.close()
    exit(0)