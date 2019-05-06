# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import keys
import requests
import time
import random
import datetime
import gecodeconvert as gc


class Amap:

    def __init__(self):
        self.key_index = 0
        self.key_switch = False
        self.keys = keys.amap[self.key_index % len(keys.amap)]
        self.search_base = 'https://restapi.amap.com/v3/place/text?'

    def search_location_api_call(self, amap_key, **kwargs):
        query = 'https://restapi.amap.com/v3/place/text?'# keywords=北京大学&city=beijing&output=xml&offset=20&page=1&key=<用户的key>&extensions=all

        for key, value in kwargs.items():
            query = query + '&' + key + '=' + str(value)
        query = query + '&key=' + amap_key
        response = requests.get(query).json()
        time.sleep(random.randint(1, 2))

        if ('status' not in response.keys()) or (response['status'] != '1') or (float(response['count']) < 1):
            return None
        else:
            one_call = pd.DataFrame()
            for record in response['pois']:
                mapit = self.geocode_convert(float(record['location'].split(',')[0]), float(record['location'].split(',')[1]))
                record['MapITlat'] = mapit[1]
                record['MapITlon'] = mapit[0]

                one_call = one_call.append(record, ignore_index=True)

            return one_call

        # return response


    def get_api_call(self, api_response):
        one_call = pd.DataFrame()
        if ('status' not in api_response.keys()) or (api_response['status'] != '1') or (float(api_response['count']) < 1):
            return None
        else:
            for store in api_response['pois']:
                mapit = self.geocode_convert(float(store['location'].split(',')[0]), float(store['location'].split(',')[1]))
                store['MapITlat'] = mapit[1]
                store['MapITlon'] = mapit[0]

                one_call = one_call.append(store, ignore_index=True)

            return one_call

    # Convert from Baidu to
    def geocode_convert(self, lat, lon):

        return gc.gcj02_to_wgs84(lon, lat)


if __name__ == '__main__':
    site = 'FirePublic'
    date = str(datetime.date.today())
    property_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\FirePublic_2019-03-24.xlsx', sheet_name='FirePublic', sort=False)
    # property_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\LawFirm_gz_2019-03-01_0.xlsx', sheet_name='Sheet1',  sort=False)

    # keywords = ['鹿角巷', 'gaga鲜语', '安缇安蝴蝶饼', '卅卅红油串串', '乐刻健身', '披萨工坊', '瑞幸咖啡', 'luckin coffee', '沃尔玛', '美吉姆', '星巴克', 'Starbucks', '斯凯奇', '全家便利店', 'FamilyMart',
    #             '天虹百货', '盒马鲜生', 'FILA', '阿迪达斯', 'ADIDAS', '喜茶', 'Teenie Weenie', '古德菲力', '7fresh', 'Innisfree', 'Pandora', '豪客来', 'KOI', '7-11', '7-ELEVEN', '大家乐', '乐凯撒', '马克华菲',
    #             '钻石世家', 'OCE', '蛙来哒']

    # 广州, 深圳, 东莞, 佛山, 惠州, 江门, 中山, 珠海, 肇庆
    cities = ['440100']

    df = pd.DataFrame()
    # for city in cities:
    #     for key in keywords:
    #         page = 1
    #         while page > 0:
    #             response = search_location_api_call(keys.amap, keywords=key, types='120000', offset='1', output='JSON', page=page) #  building='B0FFH11BOI',
    #             one_call = get_api_call(response)
    #             if one_call is not None:
    #                 one_call['Keyword'] = key
    #                 df = df.append(one_call)
    #                 page += 1
    #             else:
    #                 break
    count = 0
    amp = Amap()
    for index, property in property_list.iterrows():

        keyword = str(property['地址'])
        response = amp.search_location_api_call(keys.amap, keywords=keyword, city=cities[0], citylimit=True, types='120000', offset='1', output='JSON') #  building='B0FFH11BOI',
        one_call = amp.get_api_call(response)
        if one_call is None:
            one_call = dict()

        one_call['Source_ID'] = property['Source_ID']

        df = df.append(one_call, ignore_index=True)

        # count += 1
        # if count >= 10:
        #     break

    df['Timestamp'] = date
    # df = df.drop_duplicates(subset=['id'], keep='first')
    df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_Amap_{}.xlsx'.format(site, date), index=False, header=True, columns=list(df), sheet_name='Amap Api')


