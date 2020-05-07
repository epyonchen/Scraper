# -*- coding: utf-8 -*-
"""
Created on Sun June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import keys
import random
import time
import urllib
import gecodeconvert as gc
from default_api import default_api
from utility_commons import getLogger, get_nested_value

logger = getLogger('Amap')


class Amap(default_api):
    # Map api type to input parameter
    _api_keys = {
        'text': ['keywords', 'types', 'city'],  # specific keyword
        'around': ['keywords', 'types', 'location', 'radius', 'city'],  # poi of round area, <lon, lat>
        'polygon': ['keywords', 'types', 'polygon', 'city'],  # poi of polygon area
        'detail': ['id']
    }

    _default_kwargs = {
        'citylimit': True,
        'offset': '10',
        'output': 'JSON',
        'page': 0,
        'key': keys.amap['map_ak']
    }

    _alter_kwargs = {
        'sign': 'sig',
        'keyword': 'keywords',
        'page': 'page',
        'lat': 'lat',
        'lon': 'lon'}

    def __init__(self, api='text'):
        super().__init__(api)
        self.base = 'https://restapi.amap.com/v3/place/{}?'.format(api)

    @staticmethod
    def geocode_convert(lon, lat):
        return pd.Series(gc.gcj02_to_wgs84(lon, lat))

    def query(self, source_df, **kwargs):
        results = super(Amap, self).query(source_df=source_df, **kwargs)
        if not results.empty:
            results[['lon', 'lat']] = results['location'].str.split(',', 1, expand=True)
            results[['MapIT_lon', 'MatIT_lat']] = results.apply(
                lambda x: self.geocode_convert(float(x['lon']), float(x['lat'])),
                axis=1)

        return results

    def _get_sign(self, query):
        query = ''
        for key, value in self.parameters.items():
            query = query + '&' + key + '=' + str(value)
        raw_str = query + keys.amap['map_sk']

        return self.get_md5(raw_str)

    def validate_response(self, api_response):
        # Validate response
        if not api_response:
            logger.error('No response from api.')
            return None
        elif api_response['status'] != '1':
            logger.error('Response error, status: {}'.format(api_response['status']))
        else:
            one_call = list()
            for result in api_response['pois']:
                flat_record = get_nested_value(result)
                one_call.append(flat_record)

            return one_call


if __name__ == '__main__':
    import pandas as pd
    amp = Amap('text')
    # input = excel_to_df('Guangzhou Project ID')
    df = pd.DataFrame()
    df = df.append({'keywords': '喜茶', 'city': '广州'}, ignore_index=True)
    print(amp.query(df))
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


