"""
Created on Jun 21st 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


import time
import random
from scrapers import Scraper
from utility_commons import *


class CityRenewal(Scraper):
    def __init__(self, city='sz'):
        Scraper.__init__(self, city)
        self.search_base = 'http://csgx.szhome.com/detail'
        self.search_url = '/{}.html#ad-image-0'
        # self.browser = pm.Page()
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cookie': 'global_cookie=p3w6bio2f7pgtem9ve00ojxp610k9jjesaa; newhouse_user_guid=2040B0AC-E560-1489-186B-580AB8351580; Integrateactivity=notincludemc; city=sz; __utmc=147393320; cloudtypeforb=2; keyWord_recenthousesz=%5b%7b%22name%22%3a%22%e8%a5%bf%e4%b9%a1%22%2c%22detailName%22%3a%22%e5%ae%9d%e5%ae%89%22%2c%22url%22%3a%22%2fhouse-a089-b02092%2f%22%2c%22sort%22%3a2%7d%5d; integratecover=1; Captcha=396A6A74645A663744307A4B425A4F6F784A3969766C76676F674E4845745167306451456A726E4F5376344934546C7832376B64682B6D4F55654244793844557554446D6C4B42417A43673D; _sf_group_flag=xf; sourcepage=logout_home%5Elb_jingjipc%7Clogout_home%5Elb_kfypc; csrfToken=VniWvkzc-ePZckFnm58TxoRy; __utma=147393320.616529033.1588055785.1591083518.1591086657.5; __utmz=147393320.1591086657.5.5.utmcsr=fangjia.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/zoushi/c0sz/a089-b02092/; logGuid=82e93004-820b-4634-a468-1f0f431dbde1; clickallhousetab=1; g_sourcepage=esf_fy%5Elb_pc; __utmt_t0=1; __utmt_t1=1; __utmt_t2=1; unique_cookie=U_iob0ap3r00h9ctjnwk3ihmf7k1pkaxkwnsx*41; __utmb=147393320.94.10.1591086657',
            'dnt': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
        }

    def get_item_list(self, id, **kwargs):
        time.sleep(random.randint(0, 1))
        list_link = self.search_url.format(id)
        list_soup = self.search(self.search_base + list_link)
        if list_soup.text == 'id不存在':
            return None

        item = dict()
        item['名称'] = list_soup.find('h1', attrs={'class': 'f26'}).text
        ad_title, ad_value = list_soup.find('p', attrs={'class': 'topinfo'}).text.split('：')
        item[ad_title] = ad_value
        item_list = list_soup.find_all('li', attrs={'class': 'co1'})

        for row in item_list:
            title, value = row.text.split('：')
            item[title] = value

        des_list = list_soup.find('div', attrs={'class': 'bg-fff mb20 cloum2 f14 csgxinfo'}).find_all('p')
        description = ''
        for des in des_list:
            description = description + des.text
        item['项目介绍'] = description.strip()
        try:
            pic = list_soup.find('div', attrs={'id': 'gallery'}).find('img')
            item['图片'] = pic['src']
        except Exception as e:
            item['图片'] = None

        return [item]


if __name__ == '__main__':
    one_entity, start, end = CityRenewal.run(entity='sz', from_page=1, to_page=1000)
    df_to_excel(one_entity.df, '深圳城市更新')
