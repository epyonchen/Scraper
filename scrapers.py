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


class Scraper:
    def __init__(self, entity=None):
        self.search_base = None
        self.search_url = None
        self.entity = entity
        self.df = pd.DataFrame()
        self.info = pd.DataFrame()
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()
        self.switch = True

    # Query one link
    def search(self, url=None, encoding=None, headers=None):
        if not url:
            logger.exception('Search url missing.')
            return False

        # renew session one time if error
        while True:
            try:
                response = requests.get(url, headers=headers)
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
                    logger.exception(e)
                    return False

    # Renew session and cookies
    def renew_session(self):
        if self.search_base is None:
            logger.exception('Search base is None.')
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
            logger.exception(e)
            return False

    # Get item list in one page for one entity
    def get_item_list(self, entity, pagenum):
        item_list = pagenum
        return item_list

    # Format dataframe into db structure
    def format_df(self):

        return self.df

    @classmethod
    def run(cls, from_page=1, to_page=None, entity=None, step=1):
        page = from_page
        one_entity = cls(entity)
        item_info_load = []
        item_detail_load = []

        if entity is None:
            logger.exception('Entity is missing.')
            return False, str(from_page), str(page)

        logger.info('Start querying {}.'.format(entity))

        while (to_page is None) or (page <= to_page):
            logger.info('Query entity: {}    Page: {}.'.format(entity, page))

            # Get items in one page
            item_list = one_entity.get_item_list(entity, page)
            page += step

        logger.info('Total {} records, {} pages.'.format(str(len(item_list)), str(page - from_page)))

        if bool(item_list):
            one_entity.df = one_entity.df.append(item_list, ignore_index=True, sort=False)
        if bool(item_info_load):
            one_entity.info = one_entity.info.append(item_info_load, ignore_index=True, sort=False)
        one_entity.df = one_entity.format_df()
        return one_entity, str(from_page), str(page - 1)


class TwoStepScraper(Scraper):
    def __init__(self, city):
        self.search_base = None
        self.search_url = None
        self.city = city
        self.df = pd.DataFrame()
        self.info = pd.DataFrame()
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()
        self.switch = True

    # Query one city
    @classmethod
    def run(cls, from_page=1, to_page=None, city=None, step=1):
        page = from_page
        one_city = cls(city)
        item_info_load = []
        item_detail_load = []

        if city is None:
            logger.exception('City is missing.')
            return False, str(from_page), str(page)

        logger.info('Start querying {}.'.format(city))

        while (to_page is None) or (page <= to_page):
            logger.info('Query City: {}    Page: {}.'.format(city, page))

            # Get items in one page
            item_list = one_city.get_item_list(city, page)
            page += step

            # If item_list is empty, stop query
            if not bool(item_list):
                logger.info('Page {} is empty. Stop this job.'.format(page - 1))
                logger.info('Total {} records, {} pages.'.format(str(len(item_detail_load)), str(page - from_page)))
                if item_detail_load != []:
                    one_city.df = one_city.df.append(item_detail_load, ignore_index=True, sort=False)
                return one_city, str(from_page), str(page - 1)
            else:
                # Go through items in one page
                for item in item_list:
                    # Get detail list of one item
                    item_detail_list = one_city.get_item_detail(item)
                    if not item_detail_list:
                        continue
                    else:
                        item_detail_load += item_detail_list

        logger.info('Total {} records, {} pages.'.format(str(len(item_detail_load)), str(page - from_page)))

        if bool(item_detail_load):
            one_city.df = one_city.df.append(item_detail_load, ignore_index=True, sort=False)
        if bool(item_info_load):
            one_city.info = one_city.info.append(item_info_load, ignore_index=True, sort=False)
        one_city.df = one_city.format_df()
        return one_city, str(from_page), str(page - 1)
    
    # Get item information
    def get_item_info(self):
        item_info = None
        return item_info
        
    # Get detail of one item
    def get_item_detail(self, item):
        item_detail = item
        return item_detail

# TODO: Get item detail in separete table