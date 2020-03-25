"""
Created on Mon July 1st 2019

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

SITE = 'Expo'
TABLENAME = 'Scrapy_' + SITE
LOG_PATH = LOG_DIR + '\\' + SITE + '.log'

logger = getLogger(SITE)

class Expo(TwoStepScraper):
    def __init__(self, city):
        TwoStepScraper.__init__(self, city)
        self.search_base = 'http://www.onezh.com'
        self.search_suffix = '/zhanhui/{}_423_424_0_20190101/20191231/'

    # Get items in one page
    def get_item_list(self, pagenum):
        list_link = self.search_suffix.format(pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('div', attrs={'class': 'row'})
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        time.sleep(random.randint(0, 1) / 5.0)
        item_link = item.find('a')['href']
        try:
            item_id = re.compile(r'\d+').search(item_link).group(0)
        except Exception as e:
            print(e)
            return False
        item_name = item.find('a')['title']
        print(item_name)
        item_detail_list = []
        one_item_soup = self.search(self.search_base + item_link)

        try:
            detail_list = one_item_soup.find_all('dl', attrs={'class': re.compile(r'tuan-info.*')})
            # logger.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logger.error(e)
            return False

        # Go through detail list of one item
        item_detail = dict()

        for row in detail_list:

            title = row.find('dt', attrs={'class': re.compile(r'tuan-name')}).text.replace('：', '')
            info = row.find('dd')
            if title == '展会时间':
                item_detail[title] = re.sub(r'\s', '', info.div.text).replace('纠错', '')
            elif title == '展会地点':
                if info.find('a'):
                    item_detail[title] = info.find('a').text
                else:
                    item_detail[title] = info.text
            elif title == '组织机构':
                og = re.compile(r'承办单位：.+')
                try:
                    og0 = og.sub('', info.text).strip()
                    item_detail[og0.split('：')[0]] = og0.split('：')[1]
                except:
                    1
                try:
                    og1 = og.search(info.text).group(0).strip()
                    item_detail[og1.split('：')[0]] = og1.split('：')[1]
                except:
                    1

        try:
            detail_list2 = one_item_soup.find('div', attrs={'class': 'tuan-dside'}).find_all('li')
            # logger.info('Building Name: {}     Office Count: {}'.format(item_name, len(detail_list)))
        except Exception as e:
            logger.error(e)
            return False

        for row2 in detail_list2:
            if re.search(r'：', row2.text):
                item_detail[re.split(r'：', row2.text)[0].strip()] = re.split(r'：', row2.text)[1].strip()
        item_detail['ID'] = item_id
        item_detail['展会名称'] = item_name

        return [item_detail]

if __name__ == '__main__':
    one_city, start, end = timeout(func=Expo.run, time=18000, city='gz')
    one_city.df.to_excel(r'C:\Users\Benson.Chen\Desktop\Expo_GZ.xlsx', index=False, header=True, sheet_name='EXPO')
