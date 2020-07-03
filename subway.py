"""
Created on Aug 21st 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""


import re
import requests
from utility_commons import *
from urllib.parse import quote
from scrapers import Scraper


class Subway(Scraper):

    def __init__(self, entity=None):
        self.search_base = 'https://en.wikipedia.org/wiki/'
        self.search_url = quote(entity)
        self.entity = entity
        self.df = pd.DataFrame()
        self.info = pd.DataFrame()
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()
        self.switch = True

    def get_item_list(self, headers=None):
        items_soup = self.search(self.search_base + self.search_url, headers=headers)
        items_table = items_soup.find_all('table', attrs={'class': 'wikitable'})

        if not bool(items_table):
            return False
        elif len(items_table) > 1:
            items_table = items_table[-1]
        # Column name
        items_table = items_table.find_all('tr')
        col_names = items_table[0].find_all('th')
        col_names = list(map(lambda x: re.sub(r'\[.*\]', '', x.text.strip()), col_names))
        while '' in col_names:
            col_names.remove('')

        item_detail_list = []
        repeated_detail = {}
        for item in items_table[2:]:
            print(item.text)
            if len(item.contents) < 3:
                continue
            col_num = 0
            item_detail = {}
            rd_bk = repeated_detail.copy()

            for key, value in rd_bk.items():
                if value['count'] > 0:
                    item_detail[key] = value['text']
                    value['count'] -= 1
                    if value['count'] < 1:
                        del repeated_detail[key]
                else:
                    del repeated_detail[key]

            for tag in item.find_all(re.compile('t.*')):
                text = re.sub(r'\s', '', tag.text)
                if (not tag.name) or ((text == '') and (col_num < 2)):
                    continue
                elif tag.name == 'th':
                    item_detail[col_names[col_num]] = text
                    col_num += 1
                elif tag.name == 'td':
                    print(item_detail)
                    while col_names[col_num] in repeated_detail.keys():
                        col_num += 1

                    if 'rowspan' in tag.attrs:
                        repeated_detail[col_names[col_num]] = {'text': text, 'count': int(tag['rowspan'])}
                    item_detail[col_names[col_num]] = text
                    col_num += 1

            item_detail['线路'] = self.entity
            item_detail_list.append(item_detail)

        return item_detail_list


if __name__ == '__main__':
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'accept-encoding': 'gzip, deflate, br',
        # 'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'max-age=0',
        'cookie': 'TBLkisOn=0; mwPhp7Seed=8db; GeoIP=CN:SH:Shanghai:31.04:121.40:v4; WMF-Last-Access-Global=22-Aug-2019; WMF-Last-Access=22-Aug-2019',
        'dnt': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'

    }
    result_list = []
    for i in range(1, 23):
        entity = 'Line_{}_(Guangzhou_Metro)'.format(i)
        s = Subway(entity)
        print(entity)
        result = s.get_item_list(headers=None)
        if result:
            result_list = result_list + result
        else:
            logging.error('No response for {}'.format(entity))

    df = pd.DataFrame(result_list)
    df.fillna('')
    df['地铁站名'] = df['站名'] + df['站名及配色'] + df['站名和配色']
    # df = df.drop(['站名', '站名及配色'], axis=1)
    df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\GZ_Subway_en.xlsx', index=False)
