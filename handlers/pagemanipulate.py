# -*- coding: utf-8 -*-
"""
Created on June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""


from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from utils.utility_log import get_logger
from utils.utility_commons import get_geckodriver

_DEFAULT_PREFERENCE = {
    'browser.download.folderList': 2,
    'browser.download.manager.showWhenStarting': False,
    # 'browser.download.dir': DOWNLOAD_PATH,
    'browser.download.manager.closeWhenDone': True,
    'browser.download.manager.focusWhenStarting': False,
    'browser.helperApps.neverAsk.saveToDisk': 'text/csv/xls/xlsx'
}

logger = get_logger(__name__)
# Confirm geckodriver is installed
if not get_geckodriver():
    exit(1)

class Page:

    def __init__(self, url='http://www.example.com', page_load_strategy='eager', **preference):
        logger.info('Open browser.')
        self.renew(url, page_load_strategy, **preference)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.exception('{}, {}, {}'.format(exc_type, exc_val, exc_tb))
        self.close()
        # logger.info('Close browser.')

    def exist(self, path):
        try:
            tab = self.driver.find_element_by_xpath(path)
            return tab
        except NoSuchElementException:
            logger.info('Xpath: {} not exists.'.format(path))
            return False

    def get(self, url):
        self.base = url
        try:
            self.driver.get(self.base)
            self.soup = self.driver.page_source
        except Exception:
            logger.exception('Fail to open url {}'.format(url))
            exit(1)

    def click(self, path):
        while True:
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.element_to_be_clickable((By.XPATH, path)))
                self.driver.find_element_by_xpath(path).click()
                self.soup = self.driver.page_source
                # print(self.soup)
                return self.driver.page_source
            except TimeoutException:
                self.renew(self.base)
                logger.exception('Timeout')
                # return False

    def send(self, path, value):
        try:
            if self.exist(path):
                self.driver.find_element_by_xpath(path).send_keys(value)
        except Exception:
            logger.exception('Fail to send {} to {}'.format(value, path))

    def renew(self, url='http://www.example.com', page_load_strategy='eager', **preference):
        if hasattr(self, 'driver'):
            self.close()
            logger.info('Renew browser.')
        self.desired_capabilities = DesiredCapabilities.FIREFOX
        self.desired_capabilities["pageLoadStrategy"] = page_load_strategy
        self.options = webdriver.FirefoxOptions()
        self.options.add_argument('-headless')
        if not bool(preference):
            _DEFAULT_PREFERENCE.update(preference)
        self.profile = webdriver.FirefoxProfile()
        for key, value in _DEFAULT_PREFERENCE.items():
            self.profile.set_preference(key, value)
        self.driver = webdriver.Firefox(firefox_options=self.options, firefox_profile=self.profile)
        # self.driver.maximize_window()
        self.soup = None
        self.driver.get(url)

    def close(self):
        logger.info('Close browser.')
        self.driver.close()

    def get_requests_cookies(self):
        import requests
        webdriver_cookies = self.driver.get_cookies()
        cookies = requests.Session().cookies

        for c in webdriver_cookies:
            cookies.set(c["name"], c['value'])
        return cookies


