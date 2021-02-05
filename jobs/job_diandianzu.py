"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""
# -*- coding: utf-8 -*-

import job_libs
import utils.utility_email as em
from handlers.diandianzu import Diandianzu
from handlers.db import Mssql, get_sql_list
from utils.utility_commons import TIME, PATH, DB
import keys

SITE = 'diandianzu'
DB['DETAIL_TABLE'] = 'Scrapy_' + SITE
DB['INFO_TABLE'] = 'Scrapy_' + SITE + '_Info'
PATH['LOG_PATH'] = PATH['LOG_DIR'] + '\\' + SITE + '.log'

with Mssql(config=keys.dbconfig_mkt) as exist_db:
    condition_full = '[Source] = {0}'.format(get_sql_list(SITE))
    condition_done = '[Timestamp] >= {0} AND '.format(get_sql_list(TIME['TODAY'])) + condition_full
    entities_full = exist_db.get_logs(table_name=DB['LOG_TABLE_NAME'], condition=condition_full)
    entities_done = exist_db.get_logs(table_name=DB['LOG_TABLE_NAME'], condition=condition_done)
    entities_run = list(set(entities_full) - set(entities_done if entities_done else []))

for entity in entities_run:
    entity_object = Diandianzu(entity)
    entity_object.run()

    with Mssql(config=keys.dbconfig_mkt) as entity_db:
        entity_db.upload(df=entity_object.df['df'], table_name=DB['DETAIL_TABLE'], new_id=SITE)
        entity_db.upload(df=entity_object.df['info'], table_name=DB['INFO_TABLE'], new_id=SITE, dedupe_col='Source_ID')
        entity_db.log(Entity=entity, Timestamp=TIME['TODAY'], Source=SITE, start=1, end=len(entity_object.df['info']))

scrapyemail = em.Email()
scrapyemail.send(subject='[Scrapy] ' + DB['DETAIL_TABLE'], content='Done', attachment=PATH['LOG_PATH'])
scrapyemail.close()
exit(0)
