# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import keys
import requests
import time
import random
import pandas as pd
import gecodeconvert as gc
from utility_commons import *

logger = getLogger('Amap')


def geocode_convert(lat, lon):

    return gc.gcj02_to_wgs84(lon, lat)


class Amap:
    # Map api type to input parameter
    _api_keys_map = {'text': ['keywords', 'types', 'city'],  # return specific keyword
                     'around': ['keywords', 'types', 'location', 'radius', 'city'],  # return poi of round area
                     'polygon': ['keywords', 'types', 'polygon', 'city'],  # return poi of polygon area
                     'detail': ['id']
                     }

    _default_kwargs = {'citylimit': True,
                       'offset': '1',
                       'output': 'JSON',
                       'page': '0'
                      }

    def __init__(self, api='text'):
        self.key_index = 0
        self.key_switch = False
        self.keys = keys.amap[self.key_index % len(keys.amap)]
        self.api_type = api
        self.search_base = 'https://restapi.amap.com/v3/place/{}?'.format(api)

    # Single query of api
    def call_api(self, amap_key, **kwargs):
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
            one_call = get_nested_value(response['pois'])
            return one_call

    # Convert from Amp to wgs

    # Update input parameters
    def update_parameters(self, source_row=None, **kwargs):
        parameters = self._default_kwargs.copy()
        if source_row:
            if set(self._api_keys_map.keys()).intersection(list(source_row)):
                for key, value in self._api_keys_map.items():
                    parameters.update({key: str(source_row[value])})
            else:
                logger.error('Valid query keyword is missing in source.')
                return None
        if kwargs:
            parameters.update(kwargs)

        return parameters

    # Do api query
    def query(self, source_df, **kwargs):
        results = pd.DataFrame()
        self._default_kwargs = self.update_parameters(**kwargs)
        for index, row in source_df.iterrows():
            parameters = self.update_parameters(row)
            if parameters:
                logger.info('Running index: {}'.format(index))
            else:
                return None

            while True:
                one_call = amp.call_api(keys.amap, **parameters)
                if one_call:
                    one_call.update(row.to_dict())
                    results = results.append(one_call, ignore_index=True)
                else:
                    break
                if parameters['page'] > 0:
                    parameters['page'] += 1
                else:
                    break
        return results


if __name__ == '__main__':
    amp = Amap()
    input = excel_to_df('Guangzhou Project ID')

    # input = pd.read_excel(FILE_DIR + r'\Guangzhou Project ID.xlsx', sort=False)
    # city = '440100'
    # count = 0
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

    # input = pd.read_excel(TARGET_DIR + r'\广州2018_2019开盘住宅.xls', sort=False)
    # city = '440100'
    # for index, row in input.iterrows():
    #     keyword = str(row['项目名称'])
    #     print(keyword)
    #     one_call = amp.search_location_api_call(keys.amap[0], keywords=keyword, city=city, citylimit=True,  offset='1', output='JSON') #  building='B0FFH11BOI',types='190100',
    #     if not one_call:
    #         one_call = dict()
    #     else:
    #         one_call = one_call[0]
    #     # print(one_call)
    #     one_call.update(row.to_dict())
    #     df = df.append(one_call, ignore_index=True)
    # df = df.append(one_call, ignore_index=True)


    # type = '170000'
    # city = '441400'
    # page = 1
    # # polygon = '113.249735,23.106962|113.250146,23.106072|113.279243,23.11408|113.287617,23.110736|113.288253,23.112808|113.279949,23.115966|113.266173,23.11496|113.249735,23.106962'
    # while page > 0:
    #     # one_call = amp.search_location_api_call(polygon=polygon, amap_key=keys.amap[0], types='050000', city=city, citylimit=True, offset='20', output='JSON', page=page)  # building='B0FFH11BOI',types='190100',
    #     one_call = amp.search_location_api_call(keys.amap[0],
    #                                             location='24.003553,115.977381',
    #                                             radius='3000',
    #                                             types=type,
    #                                             city=city, citylimit=True,
    #                                             offset='25', output='JSON', page=page)
    #     if bool(one_call):
    #         print(page)
    #         print(one_call)
    #         df = df.append(one_call, ignore_index=True)
    #         page += 1
    #     else:
    #         break

    # amp.query()
    # a = pd.DataFrame([{'a':1, 'b':1}, {'a':2, 'b':2}])
    # b = ['a']
    # for index, row in a.iterrows():
    #     if set(list(row)).intersection(b):
    #         print(1)
    #     else:
    #         print(2)
    # if 'location' in list(df):
    #     df[['lat', 'lon']] = df['location'].str.split(',', expand=True)
    #     mapit = pd.DataFrame(df.apply(lambda x: amp.geocode_convert(float(x['lon']), float(x['lat'])), axis=1).values.tolist(), columns=['MapitLon', 'MapitLat'])
    #     df = pd.concat([df, mapit], axis=1)
    #
    # df['Timestamp'] = TIMESTAMP
    #
    # site = 'Geocode'
    # df.to_excel(r'C:\Users\benson.chen\Desktop\Scraper\广州2018_2019开盘住宅.xlsx', index=False, header=True, columns=list(df), sheet_name=site)
    # df.to_excel(FILE_DIR + r'\{}_Amap_{}.xlsx'.format(site, TODAY), index=False, header=True, columns=list(df), sheet_name='Amap Api')


