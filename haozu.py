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


SITE = 'Haozu'
TABLENAME = 'Scrapy_' + SITE
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)



class Haozu(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://www.haozu.com'
        self.search_url = '/{}/zuxiezilou/a1/o{}/'
        # search_url = search_base + '/{}/zuxiezilou/a1/o{}/'.format(city, str(page))

    # Get items in one page
    def get_item_list(self, pagenum):
        list_link = self.search_url.format(self.city, pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('h1', attrs={'class': 'h1-title'})
        if len(item_list) > 0:
            item_list = item_list[1:]
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1))

        item_link = item.a['href']
        item_id = str(re.compile(r'\d+').search(item_link).group(0))
        item_name = item.a.text
        item_detail_list = []

        one_item_soup = self.search(self.search_base + item_link)
        try:
            detail_list = one_item_soup.find('div', attrs={'id': 'normal-house-div'}).find_all('tr', attrs={'data-role': 'item'})
            logger.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logger.error(e)
            return False

        # Go through detail list of one item
        for row in detail_list:
            item_detail = dict()
            if 'data-content' in row.attrs.keys():
                item_detail['Source_ID'] = self.city + '_' + re.compile(r'house\d+').search(row.attrs['onclick']).group(0)
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
                        except:
                            item_detail[col['title']] = 0
                    else:
                        item_detail[col['title']] = col.i.text

            item_detail['Property'] = item_name
            item_detail['Property_ID'] = self.city + '_' + item_id
            item_detail_list.append(item_detail)
        return item_detail_list

    # Format dataframe into db structure
    # def format_df(self):
    #     print(list(self.df))
    #     if '每平米每天租金' in list(self.df):
    #         self.df.loc[self.df['每平米每月租金'], '每平米每月租金'] = self.df['每月租金/价格'].astype(float)/self.df['面积/平米'].astype(float)
    #         self.df = self.df.drop('每平米每天租金', axis=1)
    #     print(list(self.df))
    #     return self.df


if __name__ == '__main__':

    cities = ['gz', 'sz', 'bj', 'sh', 'cd']  #
    with db.Mssql(config=keys.dbconfig_mkt) as scrapydb:
        existing_cities = scrapydb.select(table_name=LOG_TABLE_NAME, source=SITE, customized={'Timestamp': ">='{}'".format(TODAY), 'City': 'IN ({})'.format('\'' + '\', \''.join(list(cities)) + '\'')})
        cities_run = list(set(cities) - set(existing_cities['City'].values.tolist()))

    for city in cities_run:
        one_city, start, end = timeout(func=Haozu.run, time=18000, city=city)  #
        logger.info('Start from page {}, stop at page {}.'.format(start, end))

        with db.Mssql(config=keys.dbconfig_mkt) as scrapydb:
            scrapydb.upload(df=one_city.df, table_name=TABLENAME, schema='CHN_MKT', start=start, end=end, timestamp=TIMESTAMP, source=SITE, entity=city)
            #
            # one_city, start, end = Haozu.run(city=city)  #, from_page=1, to_page=1
            # logger.info('Start from page {}, stop at page {}.'.format(start, end))
            # scrapydb.upload(one_city.df, TABLENAME, start=start, end=end, timestamp=TIMESTAMP, source=SITE, city=city)

    scrapyemail = em.Email()
    scrapyemail.send(subject='[Scrapy] ' + TABLENAME, content='Done', attachment=LOG_PATH)
    scrapyemail.close()
    exit(0)
