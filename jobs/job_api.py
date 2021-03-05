# -*- coding: utf-8 -*-
"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


from handlers.amap_api import Amap
from handlers.baidu_api import Baidu_translate, Baidu_map
from utils.utility_commons import excel_to_df, df_to_excel


amap = Amap('text')
# input_df = pd.DataFrame()
# input_df = input_df.append([{'keywords': '广东省广州市天河区体育东路160号平安大厦18楼、20-26楼', 'city': '广州'},])#, 'types': '120000'
#
plist = ['ref_Branch', 'ref_Brand', 'ref_name', 'ref_address', 'ref_pname', 'ref_cityname', 'ref_adname', 'MapIT_lon',
         'MapIT_lat']
city = ['Guangzhou', 'Shanghai', 'Shenzhen', 'Beijing', 'Nanjing', 'Suzhou', 'Chengdu', 'Tianjin', 'Qingdao', 'Wuhan']
result = pd.DataFrame(columns=plist)
for c in city:
    input_df = excel_to_df(file_name='map', sheet_name=c)

    output_df = amap.query(input_df)
    result = pd.concat([result, output_df[plist]])

df_to_excel(df=result, file_name='map', sheet_name='result-address')


# import pandas as pd
#
# df = pd.DataFrame()
# df = df.append([{'q': '', 'from': 'cn', 'to': 'en'}], ignore_index=True)
# print(df)
# bm = Baidu_translate()
# r = bm.query(df)
# print(r)