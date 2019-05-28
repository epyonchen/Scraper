"""
Created on Thur April 18th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import db
import keys
import logging


class TwoStepScraper:
    def __init__(self, city):
        self.search_base = None
        self.search_url = None
        self.city = city
        self.df = pd.DataFrame()
        # search_url = search_base + '/{}/zuxiezilou/a1/o{}/'.format(city, str(page))

    # Query one link
    def search(self, link=None, url=None, encoding=None):
        if link is not None:
            query = self.search_base + link
        elif url is not None:
            query = url
        else:
            logging.error('Search link missing.')
            return None
        try:
            response = requests.get(query)
            if encoding is not None:
                response.encoding = encoding
            soup = BeautifulSoup(response.text, 'lxml')
            return soup
        except Exception as e:
            logging.error(e)
            return None

    # Query one city
    @classmethod
    def run(cls, from_page=1, to_page=None, city=None):
        page = from_page
        one_city = cls(city)
        item_load_list = []

        if city is None:
            logging.error('City is missing.')
            return None, from_page, page

        logging.info('Start querying {}.'.format(city))


        while (to_page is None) or (page <= to_page):
            logging.info('Query City: {}    Page: {}.'.format(city, page))

            # Get items in one page
            item_list = one_city.get_item_list(city, page)
            page += 1

            # If item_list is empty, stop query
            if not bool(item_list):
                logging.info('Page {} is empty. Stop this job.'.format(page))
                logging.info('Total {} records, {} pages.'.format(str(len(item_load_list)), str(page - from_page)))
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

        logging.info('Total {} records, {} pages.'.format(str(len(item_load_list)), str(page - from_page)))
        if not bool(item_load_list):
            one_city.df = one_city.df.append(item_load_list, ignore_index=True, sort=False)

        one_city.df = one_city.format_df()
        return one_city.df, from_page, page - 1

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