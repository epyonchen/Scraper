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
        response = requests.get(query).json()
        time.sleep(random.randint(1, 2))

        if ('status' not in response.keys()) or (response['status'] != '1'):
            return False
        else:
            one_call = response['pois']
            # for record in response['pois']:
            #             #     mapit = self.geocode_convert(float(record['location'].split(',')[0]), float(record['location'].split(',')[1]))
            #             #     record['MapITlat'] = mapit[1]
            #             #     record['MapITlon'] = mapit[0]
            #             #
            #             #     one_call = one_call.append(record, ignore_index=True)
            #     print(record)
            return one_call

        # return response


    # def get_api_call(self, api_response):
    #     one_call = pd.DataFrame()
    #
    #     if api_response is None or ('status' not in api_response.keys()) or (api_response['status'] != '1'):
    #         return None
    #     else:
    #         for store in api_response['pois']:
    #             mapit = self.geocode_convert(float(store['location'].split(',')[0]), float(store['location'].split(',')[1]))
    #             store['MapITlat'] = mapit[1]
    #             store['MapITlon'] = mapit[0]
    #             one_call = one_call.append(store, ignore_index=True)
    #
    #         return one_call

    # Convert from Baidu to
    def geocode_convert(self, lat, lon):

        return gc.gcj02_to_wgs84(lon, lat)


if __name__ == '__main__':

    df = pd.DataFrame()
    amp = Amap()
    input = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\GZ_Subway.xlsx', sort=False)

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

    for index, station in input.iterrows():
        keyword = str(station['地铁站名']) + '(地铁站)'
        print(index, keyword)
        city = str(station['Code'])
        one_call = amp.search_location_api_call(keys.amap[0], keywords=keyword, city=city, citylimit=True,  offset='1', output='JSON') #  building='B0FFH11BOI',types='190100',
        if not one_call:
            one_call = dict()
        else:
            one_call = one_call[0]

        one_call.update(station.to_dict())
        # one_call['Source_ID'] = property['Source_ID']

        df = df.append(one_call, ignore_index=True)

        # count += 1
        # if count >= 10:
        #     break

    # page = 1
    # while page > 0:
    #     one_call = amp.search_location_api_call(amap_key=keys.amap[0], types='160100', city=cities[0], citylimit=True, offset='20', output='JSON', page=page)  # building='B0FFH11BOI',types='190100',
    #     if bool(one_call):
    #         print(page)
    #         df = df.append(one_call, ignore_index=True)
    #         page += 1
    #     else:
    #         break

    df[['lat', 'lon']] = df['location'].str.split(',', expand=True)
    mapit = pd.DataFrame(df.apply(lambda x: amp.geocode_convert(float(x['lon']), float(x['lat'])), axis=1).values.tolist(), columns=['MapitLon', 'MapitLat'])
    df = pd.concat([df, mapit], axis=1)
    df['Timestamp'] = TIMESTAMP
    site = 'Subway'
    df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\{}_Amap_{}.xlsx'.format(site, TODAY), index=False, header=True, columns=list(df), sheet_name='Amap Api')


