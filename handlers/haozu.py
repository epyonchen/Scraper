"""
Created on Dec 21st 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import random
import time
import re
import utils.utility_email as em
from handlers.db import Mssql, get_sql_list
from handlers.scrapers import TwoStepScraper
from utils.utility_log import get_logger
from utils.utility_commons import PATH, TIME, DB, get_job_name
import keys

logger = get_logger(__name__)


class Haozu(TwoStepScraper):
    def __init__(self, entity):
        super().__init__()
        self.entity = entity
        self.search_base = 'https://www.haozu.com'
        self.search_suffix = '/{}/zuxiezilou/a1/'.format(entity) + 'o{}/'

    # Get items in one page
    def get_item_list(self, pagenum):
        logger.info('Query entity: {}    Page: {}.'.format(self.entity, pagenum))
        list_link = self.search_suffix.format(pagenum)
        list_soup = self.search(url=self.search_base + list_link)
        try:
            item_list = list_soup.find('ul', attrs={'class': 'listCon propertyList'}). \
                find_all('h1', attrs={'class': 'h1-title'})
        except Exception:
            logger.exception('Fail to get item list')
            return None

        return item_list[1:] if len(item_list) > 0 else None

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1))
        try:
            item_link = item.a['href']
        except Exception:
            logger.exception('Fail to get item link')
            return None

        item_id = str(re.compile(r'\d+').search(item_link).group(0))
        item_name = item.a.text
        item_detail_list = []
        one_item_soup = self.search(self.search_base + item_link)

        try:
            detail_list = one_item_soup.find('table', attrs={'id': 'normalHouseList'}). \
                find_all('tr', attrs={'data-role': 'item'})
            logger.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception:
            logger.exception('Fail to get detail list')
            return None

        # Go through detail list of one item
        for row in detail_list:
            item_detail = dict()
            if 'data-content' in row.attrs.keys():
                item_detail['Source_ID'] = self.entity + '_' + re.compile(r'house\d+'). \
                    search(row.attrs['onclick']).group(0)
            else:
                continue

            col_list = row.find_all('td')
            for col in col_list:
                if 'title' not in col.attrs:
                    continue
                else:
                    if re.compile(r'.+万元/月').search(col.text) is not None:
                        try:
                            item_detail[col['title']] = str(float(col.i.text) * 10000)
                        except Exception:
                            item_detail[col['title']] = 0
                    else:
                        item_detail[col['title']] = col.i.text

            item_detail['Property'] = item_name
            item_detail['Property_ID'] = self.entity + '_' + item_id
            item_detail_list.append(item_detail)
        item_info = self.get_item_info(one_item_soup)
        item_info['Source_ID'] = self.entity + '_' + item_id
        item_info['Property'] = item_name
        return item_detail_list, [item_info]

    def get_item_info(self, item_detail_soup):
        ad_pattern = re.compile(r'\w+')
        fl_pattern = re.compile(r'\s+')

        info = dict()
        info['City'] = self.entity
        try:
            address = item_detail_soup.find('div', attrs={'class': 'house-address'}).span.text
            address = ad_pattern.findall(address)
            info['district'] = address[0]
            info['area'] = address[1]
            info['address'] = address[2]
        except Exception:
            logger.exception('No Address info.')

        try:
            overview = item_detail_soup.find('ul', attrs={'class': 'overview'}).find_all('li')
            for desc in overview:
                if len(desc.contents) > 3:
                    info[desc.contents[1].text.strip()] = re.subn(fl_pattern, ',', desc.contents[3].text.strip())[0]
        except Exception:
            logger.exception('No overview info.')

        try:
            transportation = item_detail_soup.find('ul', attrs={'class': 'map-tips'}).find_all('li')
            for trans in transportation:
                kv = trans.find_all('span')
                info[kv[0].text] = kv[1].text
        except Exception:
            logger.exception('No transportation info.')

        return info

    def format_df(self):
        super().format_df()

        def _limit_len(df):
            col_dict = dict(
                (col, df[col].apply(lambda x: len(str(x)) if x is not None else 0).max()) for col in df.columns.values
            )
            for k, v in col_dict.items():
                if v > 255:
                    df[k] = df[k].apply(lambda x: str(x)[:255])
            return df

        self.info = _limit_len(self.info)
        return self

