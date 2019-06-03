"""
Created on Thur April 18th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import pagemanipulate as pm

logger = logging.getLogger('scrapy')


class TwoStepScraper:
    def __init__(self, city):
        self.search_base = None
        self.search_url = None
        self.city = city
        self.df = pd.DataFrame()
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()
        self.switch = True

    # Query one link
    def search(self, url=None, encoding=None):
        if not url:
            logger.error('Search url missing.')
            return False

        # renew session one time if error
        while True:
            try:
                response = requests.get(url)
                if encoding:
                    response.encoding = encoding
                soup = BeautifulSoup(response.text, 'lxml')
                self.switch = True
                return soup
            except Exception as e:
                if self.switch:
                    self.renew_session()
                    continue
                else:
                    logger.error(e)
                    return False

    # Renew session and cookies
    def renew_session(self):
        if self.search_base is None:
            logger.error('Search base is None.')
            return False
        try:
            with pm.Page(self.search_base, 'normal') as page:
                logger.info('Renew session.')
                self.cookies = page.get_requests_cookies()
                self.session = requests.Session()
                self.session.cookies.update(self.cookies)
                self.switch = False
            return True
        except Exception as e:
            logger.error(e)
            return False

    # Query one city
    @classmethod
    def run(cls, from_page=1, to_page=None, city=None):
        page = from_page
        one_city = cls(city)
        item_load_list = []

        if city is None:
            logger.error('City is missing.')
            return None, from_page, page

        logger.info('Start querying {}.'.format(city))

        while (to_page is None) or (page <= to_page):
            logger.info('Query City: {}    Page: {}.'.format(city, page))

            # Get items in one page
            item_list = one_city.get_item_list(city, page)
            page += 1

            # If item_list is empty, stop query
            if not bool(item_list):
                logger.info('Page {} is empty. Stop this job.'.format(page - 1))
                logger.info('Total {} records, {} pages.'.format(str(len(item_load_list)), str(page - from_page)))
                if item_load_list != []:
                    one_city.df = one_city.df.append(item_load_list, ignore_index=True, sort=False)
                return one_city.df, from_page, page - 1
            else:
                # Go through items in one page
                for item in item_list:
                    # Get detail list of one item
                    item_detail_list = one_city.get_item_detail(item)
                    if item_detail_list is None:
                        continue
                    else:
                        item_load_list += item_detail_list

        logger.info('Total {} records, {} pages.'.format(str(len(item_load_list)), str(page - from_page)))

        if bool(item_load_list):
            one_city.df = one_city.df.append(item_load_list, ignore_index=True, sort=False)
        one_city.df = one_city.format_df()
        return one_city.df, str(from_page), str(page - 1)

    # Get item list in one page for one city
    def get_item_list(self, cityname, pagenum):
        item_list = pagenum
        return item_list

    # Get detail of one item
    def get_item_detail(self, item):
        item_detail = item
        return item_detail

    # Format dataframe into db structure
    def format_df(self):

        return self.df

# TODO: Get item detail in separete table