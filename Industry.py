# -*- coding: UTF-8 -*-

import scrapy

from collections import OrderedDict
from datetime import date
from scrapy.http import FormRequest

# use below command to run the spider, xxx is the output file name
# scrapy runspider IndustryScraper.py -o xxx.csv
# current link is for foshan, change the link to download other city's data
BASE_URL = 'http://land.fang.com/market/440600__3_2_____1_0_1.html'
DATA_XPATH1 = 'normalize-space(//td[span[contains(text(),"'
DATA_XPATH2 = '")]])'
current_time = date.today().strftime('%Y%m%d')

DATA_LIST = [
    'Date',
    'ID',
    r'名字',
    r'地块编号',
    r'地区',
    r'所在地',
    r'总面积',
    r'建设用地面积',
    r'规划建筑面积',
    r'代征面积',
    r'容积率',
    r'绿化率',
    r'商业比例',
    r'建筑密度',
    r'限制高度',
    r'出让形式',
    r'出让年限',
    r'位置',
    r'四至',
    r'规划用途',
    r'交易状况',
    r'竞得方',
    r'起始日期',
    r'截止日期',
    r'成交日期',
    r'交易地点',
    r'起始价',
    r'成交价',
    r'楼面地价',
    r'溢价率',
    r'土地公告',
    r'咨询电话',
    r'保证金',
    r'最小加价幅度', ]


class IndustrySpider(scrapy.Spider):
    name = 'Industry'
    start_urls = [BASE_URL]

    def parse(self, response):
        for land in response.css('div.list28_text'):
            land_page = land.css('h3 a::attr("href")').extract_first()
            yield response.follow(land_page, self.parse_land)

        next_page_number = str(
            int(response.css('div.fr a.cur::text').extract_first()) + 1)
        next_page = (BASE_URL[:-6] + next_page_number + BASE_URL[-5:])

        if next_page is not None:
            yield response.follow(next_page, self.parse)

    def parse_land(self, response):
        data_dict = OrderedDict()
        data_dict['Date'] = current_time
        data_dict['ID'] = response.url[28:-5]
        data_dict['名字'] = response.css('div.tit_box01::text').extract_first()
        data_dict['地块编号'] = response.css('div.border07 '
                                         'div.menubox01.mt20 span::text').extract_first()[5:]
        for item in DATA_LIST[4:]:
            data_dict[item] = response.xpath(
                self.generate_xpath(item)).extract_first(
                default=r"暂无")[len(item) + 1:]
        yield data_dict

    def generate_xpath(self, column):
        return ''.join([DATA_XPATH1, column, DATA_XPATH2])
