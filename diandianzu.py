"""
Created on Sun Jan 9th 2019

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


class Diandianzu(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'https://{}.diandianzu.com'.format(city)
        self.search_suffix = '/listing/p{}/'

    # Get items in one page
    def get_item_list(self, cityname, pagenum):
        list_link = self.search_suffix.format(pagenum)
        list_soup = self.search(link=list_link)
        item_list = list_soup.find_all('a', attrs={'class': 'tj-pc-listingList-title-click'})
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)
        item_link = item['href']
        try:
            item_id = re.compile(r'\d+').search(item_link).group(0)
        except Exception as e:
            print(e)
            return None
        item_name = item.text
        item_detail_list = []

        one_item_soup = self.search(link=item_link)
        try:
            item_detail_title = one_item_soup.find('div', attrs={'class': 'ftitle clearfix'}).find_all('div')
        except Exception as e:
            logging.error(e)
            return None
        if not item_detail_title:
            return None
        try:
            detail_list = one_item_soup.find('div', attrs={'class': 'fbody'}).find_all('div', attrs={'class': re.compile('fitem .*')})
            logging.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logging.error(e)
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

            item_detail['Source_ID'] = city + '_' + row['data-id']
            item_detail['Property'] = item_name
            item_detail['Property_ID'] = city + '_' + item_id
            item_detail_list.append(item_detail)

        return item_detail_list


if __name__ == '__main__':
    site = 'Diandianzu'
    date = datetime.date.today().strftime('%Y-%m-%d')
    cities = ['gz', 'sz', 'sh', 'bj', 'cd']
    scrapydb = db.Mssql(keys.dbconfig)

    for city in cities:

        one_city_df, start, end = Diandianzu.run(city=city)  #, from_page=1, to_page=1
        logging.info('Start from page {}, stop at page {}.'.format(start, end))

        one_city_df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_{}_{}.xlsx'.format(site, city, date), sheet_name='{} {}'.format(site, city), index=False)

        scrapydb.upload(one_city_df, 'Scrapy_{}'.format(site), start=start, end=end, timestamp=date, source=site, city=city)
    scrapydb.close()