# -*- coding: utf-8 -*-
"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


from handlers.amap_api import Amap
from utils.utility_commons import excel_to_df, df_to_excel


amap = Amap('text')
input_df = excel_to_df(file_name='test', sheet_name='Sheet2')
result = amap.query(input_df)

# df_to_excel(df=result, file_name='电商协会_整理版')
