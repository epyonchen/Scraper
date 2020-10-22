"""
Created on Jun 21st 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


import json
import requests
import re
from scrapers import Scraper
from utility_commons import df_to_excel
from utility_log import get_logger


logger = get_logger(__name__)

class Fang(Scraper):
    def __init__(self, city):
        Scraper.__init__(self, city)
        self.search_base = 'https://dg.esf.fang.com'
        self.search_url = '/house-a0117/i3{}'
        # self.browser = pm.Page()
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cookie': 'global_cookie=p3w6bio2f7pgtem9ve00ojxp610k9jjesaa; newhouse_user_guid=2040B0AC-E560-1489-186B-580AB8351580; Integrateactivity=notincludemc; city=sz; __utmc=147393320; cloudtypeforb=2; keyWord_recenthousesz=%5b%7b%22name%22%3a%22%e8%a5%bf%e4%b9%a1%22%2c%22detailName%22%3a%22%e5%ae%9d%e5%ae%89%22%2c%22url%22%3a%22%2fhouse-a089-b02092%2f%22%2c%22sort%22%3a2%7d%5d; integratecover=1; Captcha=396A6A74645A663744307A4B425A4F6F784A3969766C76676F674E4845745167306451456A726E4F5376344934546C7832376B64682B6D4F55654244793844557554446D6C4B42417A43673D; _sf_group_flag=xf; sourcepage=logout_home%5Elb_jingjipc%7Clogout_home%5Elb_kfypc; csrfToken=VniWvkzc-ePZckFnm58TxoRy; __utma=147393320.616529033.1588055785.1591083518.1591086657.5; __utmz=147393320.1591086657.5.5.utmcsr=fangjia.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/zoushi/c0sz/a089-b02092/; logGuid=82e93004-820b-4634-a468-1f0f431dbde1; clickallhousetab=1; g_sourcepage=esf_fy%5Elb_pc; __utmt_t0=1; __utmt_t1=1; __utmt_t2=1; unique_cookie=U_iob0ap3r00h9ctjnwk3ihmf7k1pkaxkwnsx*41; __utmb=147393320.94.10.1591086657',
            'dnt': '1',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
        }

    def get_item_list(self, pagenum, **kwargs):
        column = ['户型', '面积', '楼层', '朝向', '年份', '经纪']
        list_link = self.search_url.format(pagenum)
        url = self.search_base + list_link
        response = requests.get(url, headers=self.headers)
        pattern = re.compile(r'location.href=".*"')
        redirect = pattern.search(response.text)
        if redirect:
            redirect_url = re.search(r'".*"', redirect.group(0)).group(0).replace(r'"', '')
            list_soup = self.search(url=redirect_url, headers=self.headers)
            try:
                item_list = list_soup.find_all('dl', attrs={'dataflag': 'bg'})
            except Exception:
                logger('Fail to get item list')
                return None

            if item_list is not None:
                result_list = []
                for item in item_list:
                    try:
                        house = json.loads(item['data-bg'])
                        detail = re.sub(r'\s+', '', item.find('p', attrs={'class': 'tel_shop'}).text).split('|')
                        detail.extend(['']*(len(column)-len(detail)))
                        house.update(dict(zip(column, detail)))
                        house['楼盘'] = item.find('p', attrs={'class': 'add_shop'}).a['title']
                        price = item.find('dd', attrs={'class': 'price_right'}).find_all('span')
                        house['总价'] = re.sub(r'\s+', '', price[0].text)
                        house['单价'] = re.sub(r'\s+', '', price[1].text)
                        result_list.append(house)

                    except Exception as e:
                        print(e)

                return result_list
            else:
                return None

    def format_df(self):
        self.df['总层数'] = self.df['楼层'].apply(lambda x:
                                             re.search(r'\d+', x).group(0) if re.search(r'\d+', x) else None)
        self.df['楼层'] = self.df['楼层'].apply(lambda x: re.sub(r'（.*）', '', x))
        self.df['面积'] = self.df['面积'].apply(lambda x: re.sub(r'㎡', '', x))
        self.df['单价'] = self.df['单价'].apply(lambda x: re.sub(r'元/㎡', '', x))
        self.df['总价'] = self.df['总价'].apply(lambda x: re.sub(r'万', '', x))
        self.df['年份'] = self.df['年份'].apply(lambda x: re.sub(r'年建', '', x) if re.sub(r'年建', '', x).isnumeric() else None)
        self.df = self.df.drop(['agentid', 'housetype', 'listingtype', '经纪'], axis=1)
        return self.df

if __name__ == '__main__':
    one_entity, start, end = Fang.run(entity='dg', from_page=1, to_page=43)
    df_to_excel(one_entity.df, 'fang')