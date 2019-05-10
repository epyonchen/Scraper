"""
Created on Sun Dec 21st 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import random
import time
import re
import datetime
import db
import keys
import logging
from scrapers import TwoStepScraper


class Haozu(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://www.haozu.com'
        self.search_suffix = '/{}/zuxiezilou/a1/o{}/'
        # search_url = search_base + '/{}/zuxiezilou/a1/o{}/'.format(city, str(page))

    # Get items in one page
    def get_item_list(self, cityname, pagenum):
        list_link = self.search_suffix.format(cityname, pagenum)
        list_soup = self.search(link=list_link)
        item_list = list_soup.find_all('h1', attrs={'class': 'h1-title'})
        if len(item_list) > 0:
            item_list = item_list[1:]
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)

        item_link = item.a['href']
        item_id = str(re.compile(r'\d+').search(item_link).group(0))
        item_name = item.a.text
        item_detail_list = []

        one_item_soup = self.search(link=item_link)
        try:
            detail_list = one_item_soup.find('div', attrs={'id': 'normal-house-div'}).find_all('tr', attrs={'data-role': 'item'})
            logging.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logging.error(e)
            return None

        # Go through detail list of one item
        for row in detail_list:
            item_detail = dict()
            if 'data-content' in row.attrs.keys():
                item_detail['Source_ID'] = city + '_' + re.compile(r'house\d+').search(row.attrs['onclick']).group(0)
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
            item_detail['Property_ID'] = city + '_' + item_id
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

    site = 'Haozu'
    date = datetime.date.today().strftime('%Y-%m-%d')
    cities = ['bj']  #
    scrapydb = db.Mssql(keys.dbconfig)

    for city in cities:

        one_city_df, start, end = Haozu.run(city=city)  #, from_page=1, to_page=1
        logging.info('Start from page {}, stop at page {}.'.format(start, end))

        one_city_df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)

        scrapydb.upload(one_city_df, 'Scrapy_{}'.format(site), start=start, end=end, timestamp=date, source=site, city=city)
    scrapydb.close()
