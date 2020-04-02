"""
Created on Sun Jan 9th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import random
import time
import re
import db
import keys
import utility_email as em
from utility_commons import *
from scrapers import TwoStepScraper

SITE = 'Diandianzu'
DETAIL_TABLE = 'Scrapy_Diandianzu'
INFO_TABLE = 'Scrapy_Diandianzu_Info'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(filename)s %(funcName)s %(lineno)d - %(message)s')
logger = getLogger(SITE)


class Diandianzu(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://{}.diandianzu.com'.format(city)
        self.search_suffix = '/listing/p{}/'

    # Get items in one page
    def get_item_list(self, pagenum):
        list_link = self.search_suffix.format(pagenum)
        list_soup = self.search(url=self.search_base + list_link)

        item_list = list_soup.find_all('div', attrs={'class': 'list-item-link'})#list_soup.find_all('a', attrs={'class': 'tj-pc-listingList-title-click'})
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)

        item_link = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'})['href']

        # Get item details
        item_detail_list = []

        one_item_soup = self.search(self.search_base + item_link)
        item_info = self.get_item_info(item=item, item_detail=one_item_soup)
        print(item_info)

        try:
            item_detail_title = one_item_soup.find('div', attrs={'class': 'ftitle clearfix'}).find_all('div')
        except Exception as e:
            logger.error(e)
            return False
        if not item_detail_title:
            return False
        try:
            detail_list = one_item_soup.find('div', attrs={'class': 'fbody'}).find_all('div', attrs={'class': re.compile('fitem .*')})
            logger.info('Building Name: {}     Office Count: {}'.format(item_info['Name'], len(detail_list)))
        except Exception as e:
            logger.error(e)
            return False

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

            item_detail['Source_ID'] = city + '_' + row['data-id']
            item_detail['Property'] = item_info['Name']
            item_detail['Property_ID'] = item_info['Source_ID']
            item_detail_list.append(item_detail)

        return item_detail_list, [item_info]


    def get_item_info(self, item, item_detail):
        item_region = item.find('span', attrs={'class': 'region'}).find_all('a')
        item_link = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'})['href']
        item_name = item.find('a', attrs={'class': 'tj-pc-listingList-title-click'}).text
        try:
            item_id = re.compile(r'\d+').search(item_link).group(0)
        except Exception as e:
            logger.error(e)
            return False

        item_info = dict()
        item_info['Source_ID'] = city + '_' + item_id
        item_info['Name'] = item_name
        if len(item_region) > 1:
            item_info['行政区'] = item_region[0].text
            item_info['商圈'] = item_region[1].text

        try:
            item_info_raw = item_detail.find('div', attrs={'class': 'desc-box building-box left-box clearfix'}).find_all('li')
            for info_raw in item_info_raw:
                item_info[info_raw.find('span', attrs={'class': 'f-title'}).text] = info_raw.find('span', attrs={'class': 'f-con'}).text
        except Exception as e:
            logger.error(e)

        return item_info


if __name__ == '__main__':

    cities = ['gz', 'sz',] # 'sh', 'bj', 'cd'

    with db.Mssql(config=keys.dbconfig_mkt) as scrapydb:

        existing_cities = scrapydb.select(table_name=LOG_TABLE_NAME, source=SITE, customized={'Timestamp': ">='{}'".format(TODAY), 'City': 'IN ({})'.format('\'' + '\', \''.join(list(cities)) + '\'')})
        cities_run = list(set(cities) - set(existing_cities['City'].values.tolist()))

        for city in cities_run:
            one_city, start, end = timeout(func=Diandianzu.run, time=18000, entity=city)  #
            logger.info('Start from page {}, stop at page {}.'.format(start, end))

            scrapydb.upload(df=one_city.df, table_name=DETAIL_TABLE, schema='CHN_MKT', start=start, end=end, timestamp=TIMESTAMP, source=SITE, city=city)
            scrapydb.upload(df=one_city.info, table_name=INFO_TABLE, schema='CHN_MKT', dedup=True, timestamp=TIMESTAMP, source=SITE, city=city)

    # for city in cities:
    #     one_city, start, end = timeout(func=Diandianzu.run, time=18000, entity=city, from_page=1, to_page=1)  #
    #     logger.info('Start from page {}, stop at page {}.'.format(start, end))
    #     one_city.df.to_excel(FILE_DIR + '\\' + city + DETAIL_TABLE + '.xlsx', index=False, header=True, sheet_name=city)
    #     one_city.info.to_excel(FILE_DIR + '\\' + city + INFO_TABLE + '.xlsx', index=False, header=True, sheet_name=city)
    #     break


    scrapyemail = em.Email()
    scrapyemail.send(subject='[Scrapy] ' + DETAIL_TABLE, content='Done', attachment=LOG_PATH)
    scrapyemail.close()
    exit(0)