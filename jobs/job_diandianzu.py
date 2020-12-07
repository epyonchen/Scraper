"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""
# -*- coding: utf-8 -*-

import utils.utility_email as em
from handlers.diandianzu import Diandianzu
from handlers.db import Mssql, get_sql_list
from utils.utility_commons import TIME, DB, PATH
from utils.utility_log import get_job_name
import keys

SITE = get_job_name()
PATH['DETAIL_TABLE'] = 'Scrapy_' + SITE
PATH['INFO_TABLE'] = 'Scrapy_' + SITE + '_Info'
PATH['LOG_PATH'] = PATH['LOG_DIR'] + '\\' + SITE + '.log'
cities = ['gz', 'sz', 'sh', 'bj', 'cd']

with Mssql(config=keys.dbconfig_mkt) as exist_db:
    con_city = get_sql_list(cities)
    condition = '[Timestamp] >= {0} AND [Entity] IN {1} AND [Source] = {2}'.\
        format(get_sql_list(TIME['TODAY']), get_sql_list(cities), get_sql_list(SITE))
    existing_cities = exist_db.select(table_name=DB['LOG_TABLE_NAME'], condition=condition)
    cities_run = list(set(cities) - set(existing_cities['Entity'].values.tolist()))

for city in cities_run:
    city_object = Diandianzu(city)
    city_object.run()

    with Mssql(config=keys.dbconfig_mkt) as entity_db:
        entity_db.upload(df=city_object.df, table_name=PATH['DETAIL_TABLE'], schema='CHN_MKT', new_id=SITE)
        entity_db.upload(df=city_object.info, table_name=PATH['INFO_TABLE'], schema='CHN_MKT', new_id=SITE,
                         dedupe_col='Source_ID')
        entity_db.log(Entity=city, Timestamp=TIME['TODAY'], Source=SITE, start=1, end=len(city_object.info))

scrapyemail = em.Email()
scrapyemail.send(subject='[Scrapy] ' + PATH['DETAIL_TABLE'], content='Done', attachment=PATH['LOG_PATH'])
scrapyemail.close()
exit(0)