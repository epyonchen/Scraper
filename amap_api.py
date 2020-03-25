# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import keys
import requests
import time
import random
import os
import pandas as pd
import gecodeconvert as gc
from utility_commons import *


class Amap:

    def __init__(self):
        self.key_index = 0
        self.key_switch = False
        self.keys = keys.amap[self.key_index % len(keys.amap)]
        self.search_base = 'https://restapi.amap.com/v3/place/text?'

    def search_location_api_call(self, amap_key, **kwargs):
        query = self.search_base

        for key, value in kwargs.items():
            query = query + '&' + key + '=' + str(value)
        query = query + '&key=' + amap_key

        try:
            response = requests.get(query).json()

        except Exception as e:
            logging.error(e)
            return None

        time.sleep(random.randint(1, 2))

        if ('status' not in response.keys()) or (response['status'] != '1'):
            return None
        else:
            one_call = response['pois']
            return one_call

    # Convert from Baidu to
    def geocode_convert(self, lat, lon):

        return gc.gcj02_to_wgs84(lon, lat)


if __name__ == '__main__':

    df = pd.DataFrame()
    amp = Amap()

    input = pd.read_excel(FILE_DIR + r'\Guangzhou Project ID.xlsx', sort=False)
    city = '440100'
    count = 0
    # count += 1
    # if count >= 10:
    #     break
    # keywords = input['项目']
    # for city in cities:
    #     for key in keywords:
    #         page = 1
    #         while page > 0:
    #             one_call = amp.search_location_api_call(keys.amap, keywords=key, types='120000', offset='1', output='JSON', page=page) #  building='B0FFH11BOI',
    #
    #             if one_call is not None:
    #                 one_call['Keyword'] = key
    #                 df = df.append(one_call)
    #                 page += 1
    #             else:
    #                 break

    for index, row in input.iterrows():
        keyword = str(row['项目名称'])
        print(keyword)
        one_call = amp.search_location_api_call(keys.amap[0], keywords=keyword, city=city, citylimit=True,  offset='1', output='JSON') #  building='B0FFH11BOI',types='190100',
        if not one_call:
            one_call = dict()
        else:
            one_call = one_call[0]
        # print(one_call)
        one_call.update(row.to_dict())
        df = df.append(one_call, ignore_index=True)


    # page = 1
    # polygon = '113.249735,23.106962|113.250146,23.106072|113.279243,23.11408|113.287617,23.110736|113.288253,23.112808|113.279949,23.115966|113.266173,23.11496|113.249735,23.106962'
    # while page > 0:
    #     one_call = amp.search_location_api_call(polygon=polygon, amap_key=keys.amap[0], types='050000', city=city, citylimit=True, offset='20', output='JSON', page=page)  # building='B0FFH11BOI',types='190100',
    #     if bool(one_call):
    #         print(page)
    #         df = df.append(one_call, ignore_index=True)
    #         page += 1
    #     else:
    #         break
    if 'location' in list(df):
        df[['lat', 'lon']] = df['location'].str.split(',', expand=True)
        mapit = pd.DataFrame(df.apply(lambda x: amp.geocode_convert(float(x['lon']), float(x['lat'])), axis=1).values.tolist(), columns=['MapitLon', 'MapitLat'])
        df = pd.concat([df, mapit], axis=1)

    df['Timestamp'] = TIMESTAMP

    site = 'Grade_B_Office'
    df.to_excel(FILE_DIR + r'\{}_Amap_{}.xlsx'.format(site, TODAY), index=False, header=True, columns=list(df), sheet_name='Amap Api')


