from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

class Page:

    def __init__(self, url, pageLoadStrategy='eager'):
        self.desired_capabilities = DesiredCapabilities.FIREFOX
        self.desired_capabilities["pageLoadStrategy"] = pageLoadStrategy
        self.options = webdriver.FirefoxOptions()
        self.options.add_argument('-headless')
        self.driver = webdriver.Firefox(firefox_options=self.options)
        self.soup = None
        self.driver.get(url)

    def exist(self, path):
        try:
            tab = self.driver.find_element_by_xpath(path)
            return tab
        except NoSuchElementException as e:
            return False

    def click(self, path):
        try:

            wait = WebDriverWait(self.driver, 1)
            wait.until(EC.element_to_be_clickable((By.XPATH, path)))
            self.driver.find_element_by_xpath(path).click()
            self.soup = self.driver.page_source
            print(self.soup)
            return self.driver.page_source
        except TimeoutException as e:
            return False

    def close(self):
        self.driver.close()


if __name__ == '__main__':
    url = 'http://210.76.69.38:82/JDGG/QTCGList.aspx?CGLX=A1'
    page = Page(url, 'normal')
    # page.click("//div[@class='content'")
    print(page.driver.page_source)
    # search_soup = BeautifulSoup(page.driver.page_source, 'lxml')
    #
    # office_list = search_soup.find('div', attrs={'class': 'chengxin'}).find_all('a')
    # print(office_list)
    page.close()



