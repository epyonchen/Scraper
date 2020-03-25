"""
Created on Sun Nov 5yh 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import random
import time
import requests
import db
import keys
import json
import utility_email as em
from utility_commons import *
from scrapers import TwoStepScraper
from urllib.parse import quote

SITE = 'Beike'
TABLENAME = 'Scrapy_Beike'
TABLENAME_INFO = 'Scrapy_Beike_info'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'
TEMP_PATH = FILE_DIR + '\\' + SITE + TODAY + '.xlsx'

logger = getLogger(SITE)


global cities

class Beike(TwoStepScraper):

    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://api-crep.ke.com/ke/office/building'
        self.search_url = '/list?'
        self.search_url_detail = '/detail?'

    @staticmethod
    def format_query(data):
        if not isinstance(data, dict):
            return False

        param = '&'.join([str(k) + '=' + str(v) for k, v in data.items()])

        return quote(param, safe='=')

    # Get items in one page
    def get_item_list(self, pagenum):

        time.sleep(random.randint(5, 10))

        _params_item = {
            'cityId': cities[self.city],
            'city': self.city,
            'page': pagenum,
            'delType': '2',
            'diType': '区域',
            'singlePrice': '',
            'area': '',
            'buildingSelected': '',
            'houseSelected': '',
            'fitment': '',
            'sorter': ''
        }
        list_link = self.search_base + self.search_url
        try:
            item_list = requests.get(list_link, params=_params_item)
        except Exception as e:
            logger.error('Cannot request api. {}'.format(e))
            self.success = False
            return None

        item_list = json.loads(item_list.text)

        if item_list['code'] != 0:
            return None

        return item_list['data']['docs']

    def get_item_detail(self, item):

        time.sleep(random.randint(0, 2))

        _params_detail = {
            'id': item['id'],
            'page': '1',
            'size': '1000',
            'area': '',
            'delType': '2',
            'cityId': cities[self.city]
        }

        item_info_list = item
        item_detail_list = []

        detail_link = self.search_base + self.search_url_detail
        try:
            item_details = requests.get(detail_link, params=_params_detail)
        except Exception as e:
            logger.error('Cannot request api. {}'.format(e))
            self.success = False
            return None, [item_info_list]

        item_details = json.loads(item_details.text)
        if item_details['code'] != 0:
            return item_detail_list, item_info_list

        if 'docs' in item_details['data'].keys():
            item_detail_list = item_details['data']['docs']
            del item_details['data']['docs']
        item_info_list.update(item_details['data'])

        return item_detail_list, [item_info_list]

if __name__ == '__main__':

    cities = {'广州': '440100', '深圳': '440300'}

    with db.Mssql(config=keys.dbconfig_mkt) as scrapydb:

        existing_cities = scrapydb.select(table_name=LOG_TABLE_NAME, source=SITE, customized={'Timestamp': ">='{}'".format(TODAY), 'City': 'IN ({})'.format('\'' + '\', \''.join(list(cities)) + '\'')})
        cities_run = list(set(cities) - set(existing_cities['City'].values.tolist()))

        for city in cities_run:
            one_city, start, end = timeout(func=Beike.run, time=18000, city=city)
            logger.info('Start from page {}, stop at page {}.'.format(start, end))

            if one_city.success:
                scrapydb.upload(df=one_city.df, table_name=TABLENAME, schema='CHN_MKT', start=start, end=end, timestamp=TIMESTAMP, source=SITE, city=city)
                scrapydb.upload(df=one_city.info, table_name=TABLENAME_INFO, schema='CHN_MKT', start=start, end=end, dedup=False, dedup_id='id')
            elif one_city.switch:
                one_city.df.to_excel(r'C:\Users\Benson.Chen\Desktop\detail.xlsx', index=False)
                one_city.info.to_excel(r'C:\Users\Benson.Chen\Desktop\info.xlsx', index=False)

    # for city in cities:
    #     one_city, start, end = timeout(func=Beike.run, time=18000, city=city)
    #     logger.info('Start from page {}, stop at page {}.'.format(start, end))
    #
    # one_city.df.to_excel(r'C:\Users\Benson.Chen\Desktop\detail.xlsx', index=False)
    # one_city.info.to_excel(r'C:\Users\Benson.Chen\Desktop\info.xlsx', index=False)

    scrapyemail = em.Email()
    scrapyemail.send(subject='[Scrapy] ' + TABLENAME, content='Done', attachment=LOG_PATH)
    scrapyemail.close()
    exit(0)