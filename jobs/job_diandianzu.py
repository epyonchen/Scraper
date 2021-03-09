# -*- coding: utf-8 -*-
"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


from handlers.diandianzu import Diandianzu
from handlers.db import ODBC, get_sql_list
from jobs import keys
from utils import TIME, PATH, DB, Email, get_job_name


PATH['SITE'] = 'diandianzu'
PATH['LOG_PATH'] = PATH['LOG_DIR'] + '\\' + get_job_name() + '.log'
DB['DETAIL_TABLE'] = 'Scrapy_' + PATH['SITE']
DB['INFO_TABLE'] = 'Scrapy_' + PATH['SITE'] + '_Info'


with ODBC(config=keys.dbconfig_mkt) as exist_db:
    condition_done = '[Timestamp] >= {0} AND [Source] = {1}'\
        .format(get_sql_list(TIME['TODAY']), get_sql_list(PATH['SITE']))
    entities_run = exist_db.get_to_runs(table_name=DB['LOG_TABLE_NAME'], condition=condition_done, source=PATH['SITE'])

for entity in entities_run:
    entity_object = Diandianzu(entity)
    entity_object.run()

    with ODBC(config=keys.dbconfig_mkt) as entity_db:
        entity_db.upload(df=entity_object.df['df'], table_name=DB['DETAIL_TABLE'], new_id=PATH['SITE'])
        entity_db.upload(df=entity_object.df['info'], table_name=DB['INFO_TABLE'], new_id=PATH['SITE'],
                         dedupe_col='Source_ID')
        entity_db.log(Entity=entity, Timestamp=TIME['TODAY'], Source=PATH['SITE'], start=1,
                      end=len(entity_object.df['info']))

scrapyemail = Email()
scrapyemail.send(subject='[Scrapy] ' + DB['DETAIL_TABLE'], content='Done', attachment=PATH['LOG_PATH'])
scrapyemail.close()
exit(0)
