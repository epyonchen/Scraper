"""
Created on April 18th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import handlers.pagemanipulate as pm
from func_timeout import func_set_timeout
from utils.utility_log import get_logger
from utils.utility_commons import get_job_name, TIME


logger = get_logger(__name__)


class Scraper:
    def __init__(self):
        self.search_base = None
        # self.search_url = None
        # self.entity = entity
        self.df = pd.DataFrame()
        self.info = pd.DataFrame()
        self.session = requests.session()
        self.cookies = self.session.cookies
        self.switch = True
        self.success = True

    # Query one link
    def search(self, url=None, encoding=None, headers=None):
        if not url:
            logger.error('Search url missing.')
            return None

        # renew session one time if error
        while True:
            try:
                response = requests.get(url, headers=headers)
                if encoding:
                    response.encoding = encoding
                soup = BeautifulSoup(response.text, 'lxml')
                self.switch = True
                return soup
            except Exception:
                if self.switch:
                    self.renew_session()
                    continue
                else:
                    logger.exception('Fail to request {}'.format(url))
                    return None

    # Renew session and cookies
    def renew_session(self):
        if self.search_base is None:
            logger.exception('Search base is None.')
            return None
        try:
            with pm.Page(self.search_base, 'normal') as page:
                logger.info('Renew session.')
                self.cookies = page.get_requests_cookies()
                self.session = requests.Session()
                self.session.cookies.update(self.cookies)
                self.switch = False
            return True
        except Exception:
            logger.exception('Fail to renew session.')
            return None

    # Get item list in one page for one entity
    def get_item_list(self, pagenum, **kwargs):
        item_list = pagenum
        return item_list

    # Format dataframe into db structure
    def format_df(self):
        job_name = get_job_name()
        if (self.df is not None) or (not self.df.empty):
            self.df['Timestamp'] = TIME['TODAY']
        if (self.info is not None) or (not self.info.empty):
            self.info['Timestamp'] = TIME['TODAY']

        return self

    @func_set_timeout(timeout=18000)
    def run(self, from_page=1, to_page=None, step=1):
        page = from_page
        item_info_load = []

        while (not to_page) or (page <= to_page):

            # Get items in one page
            item_list = self.get_item_list(page)
            page += step
            if item_list:

                item_info_load += item_list
            else:
                logger.info('Page {} is empty. Stop this job.'.format(page))
                break

        logger.info('Total {} records, {} pages.'.format(str(len(item_info_load)), str(page - from_page)))

        if item_info_load:
            self.df = self.df.append(item_info_load, ignore_index=True, sort=False)
        self.format_df()
        # return one_entity, str(from_page), str(page - 1)


class TwoStepScraper(Scraper):

    # Query one entity
    @func_set_timeout(timeout=18000)
    def run(self, from_page=1, to_page=None, step=1):
        page = from_page
        item_info_load = []
        item_detail_load = []

        while (not to_page) or (page <= to_page):

            # Get items in one page
            item_list = self.get_item_list(page)

            # If item_list is empty, stop query
            if not item_list:
                logger.info('Page {} is empty. Stop this job.'.format(page))
                break

            else:
                page += step
                # Go through items in one page
                for item in item_list:
                    # Get detail list of one item
                    result_tuple = self.get_item_detail(item)
                    if isinstance(result_tuple, tuple) and (len(result_tuple) > 1):
                        item_detail_list = result_tuple[0]
                        item_info_list = result_tuple[1]
                    else:
                        item_detail_list = result_tuple
                        item_info_list = None
                    if item_info_list:
                        item_info_load += item_info_list
                    if item_detail_list:
                        item_detail_load += item_detail_list
                    else:
                        continue

        logger.info('Total {} records, {} pages.'.format(str(len(item_detail_load)), str(page - from_page)))

        if item_detail_load:
            self.df = self.df.append(item_detail_load, ignore_index=True, sort=False)
        if item_info_load:
            self.info = self.info.append(item_info_load, ignore_index=True, sort=False)
        self.format_df()
        # return one_entity, str(from_page), str(page)

    # Get detail of one item
    def get_item_detail(self, item):
        item_detail = item
        return item_detail

