"""
Created on Sun July 31st 2018

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
        self.search_base = 'http://guangdong.circ.gov.cn/tabid/3173/Default.aspx'
        self.search_url = '/{}/zuxiezilou/a1/o{}/'
        # search_url = search_base + '/{}/zuxiezilou/a1/o{}/'.format(city, str(page))

    # Get items in one page
    def get_item_list(self, cityname, pagenum):
        list_link = self.search_url.format(cityname, pagenum)
        list_soup = self.search(self.search_base + list_link)
        item_list = list_soup.find_all('h1', attrs={'class': 'h1-title'})
        if len(item_list) > 0:
            item_list = item_list[1:]
        return item_list