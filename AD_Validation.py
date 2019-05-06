# -*- coding: utf-8 -*-
"""
Created on Sun March 14th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import keys
import requests
import time
import random
import datetime
import AmapApi as amap

if __name__ == '__main__':
    site = 'Property_AD'
    date = str(datetime.date.today())
    property_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Address Doctor\Address Doctor Test Case_20190308.xlsx', sheet_name='CN', sort=False)

    df = pd.DataFrame()


    count = 0
    for index, property in property_list.iterrows():

        if property['Case'] in [1, 2]:
            keyword = property['Property_Name']
            city = property['City']
        elif property['Case'] == 3:
            keyword = property['District'] + property['Address_1']
            city = property['City']
        elif property['Case'] == 4:
            keyword = property['Property_Name']
            city = property['State_Province']
        # elif property['Case'] == 6:
        #     keyword = property['District'] + property['Address_1']
        #     city = property['City']
        # elif property['Case'] == 8:
        #     keyword = property['Property_Name']
        #     city = property['State_Province']
        # elif property['Case'] == 9:
        #     keyword = property['Property_Name']
        #     city = ''

        response = amap.search_location_api_call(keys.amap2, keywords=keyword, city=city, citylimit=True, types='120000', offset='1', output='JSON') #  building='B0FFH11BOI',
        one_call = amap.get_api_call(response)
        if one_call is None:
            one_call = dict()

        one_call['Property_Code'] = property['Property_Code']
        df = df.append(one_call, ignore_index=True)
        count += 1
        # city = property['City']
        # keyword = []
        # keyword.append(property['Property_Name'])
        # keyword.append(property['District'] + property['Address_1'])
        # keyword.append(property['Property_Name'] + '|' + property['District'] + property['Address_1'])
        #
        # for kw in keyword:
        #     response = amap.search_location_api_call(keys.amap2, keywords=kw, city=city, citylimit=True, types='120000', offset='1', output='JSON')  # building='B0FFH11BOI',
        #     one_call = amap.get_api_call(response)
        #     if one_call is None:
        #         one_call = dict()
        #
        #     one_call['Property_Code'] = property['Property_Code']
        #     one_call['Input Case'] = count%3
        #
        #     df = df.append(one_call, ignore_index=True)
        #
        #
        # if count > 300:
        #     break

        if count % 100 == 0:
            df.to_excel(r'C:\Users\Benson.Chen\Desktop\Address Doctor\{}_Amap_{}.xlsx'.format(site, date), index=False, header=True, columns=list(df), sheet_name='Amap Api')

    df['Timestamp'] = date
    # df = df.drop_duplicates(subset=['id'], keep='first')
    df.to_excel(r'C:\Users\Benson.Chen\Desktop\Address Doctor\{}_Amap_{}.xlsx'.format(site, date), index=False, header=True, columns=list(df), sheet_name='Amap Api')


