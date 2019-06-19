"""
Created on Sun Dec 21st 2018

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


SITE = 'Lianjia'
TABLENAME = 'Scrapy_Lianjia'
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)


class Lianjia(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://shangye.lianjia.com'
        self.search_suffix = '/{}/xzl/rent/mlist?page={}'

    # Get items in one page
    def get_item_list(self, cityname, pagenum):
        list_link = self.search_suffix.format(cityname, pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('a', attrs={'class': 'result__li'})
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)
        item_link = item['href']
        try:
            item_id = re.compile(r'\d+').search(item_link).group(0)
        except Exception as e:
            print(e)
            return False
        item_name = item.find('p', attrs={'class': 'result__li-title'}).text
        item_detail_list = []
        one_item_soup = self.search(self.search_base + item_link)

        try:
            detail_list = one_item_soup.find_all('a', attrs={'class': 'result__li'})
            logger.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logger.error(e)
            return False

        # Go through detail list of one item
        for row in detail_list:
            item_detail = dict()
            item_detail['Property_ID'] = city + '_' + item_id
            item_detail['Property'] = item_name
            item_detail['Title'] = row.find('p', attrs={'class': 'result__li-title'}).text
            item_detail['Price'] = row.find('p', attrs={'class': 'result__li-price'}).find('span').text
            detail_link = row['href']
            item_detail['Source_ID'] = city + '_' + re.compile(r'\d+').search(detail_link).group(0)
            # Get item detail
            try:
                detail_soup = self.search(self.search_base + detail_link)
                details = detail_soup.find('div', attrs={'class': 'detail__info'}).find_all('p', recursive=False)
                for d in details:
                    item_detail[d.text.split('：')[0]] = d.text.split('：')[1]
                item_detail_list.append(item_detail)
            except Exception as e:
                logger.error(e)
                continue
        return item_detail_list


if __name__ == '__main__':

    cities = ['gz', 'sz', 'sh', 'bj', 'cd']
    with db.Mssql(keys.dbconfig) as scrapydb:

        for city in cities:
            one_city_df, start, end = Lianjia.run(city=city)  # , from_page=1, to_page=1
            logger.info('Start from page {}, stop at page {}.'.format(start, end))

            scrapydb.upload(one_city_df, TABLENAME, start=start, end=end, timestamp=TIMESTAMP, source=SITE, city=city)

    scrapyemail = em.Email()
    scrapyemail.send(TABLENAME, 'Done', LOG_PATH)
    scrapyemail.close()
