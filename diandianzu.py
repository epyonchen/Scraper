"""
Created on Jan 9th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""
# -*- coding: utf-8 -*-

import random
import time
import re
import utility_email as em
from utility_commons import PATH, TIME, DB, get_job_name
from utility_log import get_logger
from scrapers import TwoStepScraper
from db import Mssql, get_sql_list
import keys

SITE = get_job_name()
PATH['DETAIL_TABLE'] = 'Scrapy_' + SITE
PATH['INFO_TABLE'] = 'Scrapy_' + SITE + '_Info'
PATH['LOG_PATH'] = PATH['LOG_DIR'] + '\\' + SITE + '.log'
logger = get_logger(__name__)


class Diandianzu(TwoStepScraper):
    def __init__(self, entity):
        super().__init__()
        self.search_base = 'https://{}.diandianzu.com'.format(entity)
        self.search_suffix = '/listing/p{}/'
        self.entity = entity

    # Get items in one page
    def get_item_list(self, pagenum):
        logger.info('Query entity: {}    Page: {}.'.format(self.entity, pagenum))
        list_link = self.search_suffix.format(pagenum)
        list_soup = self.search(url=self.search_base + list_link)
        try:
            item_list = list_soup.find_all('div', attrs={'class': 'list-item-link'})
        except Exception:
            logger.exception('Fail to get item list')
            return None
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)

        item_link = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'})['href']

        # Get item details
        item_detail_list = []

        one_item_soup = self.search(self.search_base + item_link)
        item_info = self.get_item_info(item=item, item_detail=one_item_soup)

        try:
            item_detail_title = one_item_soup.find('div', attrs={'class': 'ftitle clearfix'}).find_all('div')
        except Exception:
            logger.exception('Fail to get title.')
            return None
        if not item_detail_title:
            return None
        try:
            detail_list = one_item_soup.find('div', attrs={'class': 'fbody'}).find_all('div', attrs={'class': re.compile('fitem .*')})
            logger.info('Building Name: {}     Office Count: {}'.format(item_info['Name'], len(detail_list)))
        except Exception:
            logger.exception('Fail to get detail list.')
            return None

        # Go through detail list of one item
        for row in detail_list:

            item_detail = dict()

            col = row.find('div')
            for title in item_detail_title:
                if title.text.strip() == '照片':
                    item_detail[title.text.strip()] = col.find('img')['src']
                else:
                    item_detail[title.text.strip()] = col.text.strip()
                col = col.find_next_sibling()

            if '单价 · 总价' in item_detail.keys():
                item_detail['单价'] = item_detail['单价 · 总价'].split()[0].strip()
                item_detail['总价'] = item_detail['单价 · 总价'].split()[1].strip()
                del item_detail['单价 · 总价']

            item_detail['Source_ID'] = self.entity + '_' + row['data-id']
            item_detail['Property'] = item_info['Name']
            item_detail['Property_ID'] = item_info['Source_ID']
            item_detail['City'] = self.entity
            item_detail_list.append(item_detail)

        return item_detail_list, [item_info]

    def get_item_info(self, item, item_detail):
        item_region = item.find('span', attrs={'class': 'region'}).find_all('a')
        item_link = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'})['href']
        item_name = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'}).text
        try:
            item_id = re.compile(r'\d+').search(item_link).group(0)
        except Exception:
            logger.exception('Fail to get item id')
            return False

        item_info = dict()
        item_info['Source_ID'] = self.entity + '_' + item_id
        item_info['Name'] = item_name
        item_info['City'] = self.entity

        if len(item_region) > 1:
            item_info['District'] = item_region[0].text
            item_info['Area'] = item_region[1].text

        try:
            item_info_raw = item_detail.find('div', attrs={'class': 'desc-box building-box left-box clearfix'}).find_all('li')
            for info_raw in item_info_raw:
                item_info[info_raw.find('span', attrs={'class': 'f-title'}).text] = info_raw.find('span', attrs={'class': 'f-con'}).text
        except Exception:
            logger.exception('Fail to get item info')

        return item_info

    def format_df(self):
        super().format_df()
        pattern = re.compile(r'\(VR看房\)')
        self.df['Property'] = self.df['Property'].apply(lambda x: re.sub(pattern, '', x))
        self.info['Name'] = self.info['Name'].apply(lambda x: re.sub(pattern, '', x))


if __name__ == '__main__':

    cities = ['gz', 'sz', 'sh', 'bj', 'cd']

    with Mssql(config=keys.dbconfig_mkt) as exist_db:
        con_city = get_sql_list(cities)
        condition = '[Timestamp] >= {0} AND [Entity] IN {1} AND [Source] = {2}'.\
            format(get_sql_list(TIME['TODAY']), get_sql_list(cities), get_sql_list(SITE))
        existing_cities = exist_db.select(table_name=DB['LOG_TABLE_NAME'], condition=condition)
        cities_run = list(set(cities) - set(existing_cities['Entity'].values.tolist()))

    for city in cities_run:
        city_object = Diandianzu(city)
        city_object.run()

        with Mssql(config=keys.dbconfig_mkt) as entity_db:
            entity_db.upload(df=city_object.df, table_name=PATH['DETAIL_TABLE'], schema='CHN_MKT', new_id=SITE)
            entity_db.upload(df=city_object.info, table_name=PATH['INFO_TABLE'], schema='CHN_MKT', new_id=SITE,
                             dedupe_col='Source_ID')
            entity_db.log(Entity=city, Timestamp=TIME['TODAY'], Source=SITE, start=1, end=len(city_object.info))

    scrapyemail = em.Email()
    scrapyemail.send(subject='[Scrapy] ' + PATH['DETAIL_TABLE'], content='Done', attachment=PATH['LOG_PATH'])
    scrapyemail.close()
    exit(0)
