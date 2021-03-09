# -*- coding: utf-8 -*-
"""
Created on Dec 7th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


import pandas as pd
from handlers.db import ODBC, get_sql_list
from handlers.pam_invoice import PAM_Invoice, logger, invoice_send_email
from jobs import keys
from utils import PATH, TIME, DB, Email, get_job_name


PATH['SITE'] = 'irregular_tax'
PATH['SCREENSHOT_PATH'] = PATH['PIC_DIR'] + r'\screen_shot.png'
PATH['VCODE_PATH'] = PATH['PIC_DIR'] + r'\vcode.png'
PATH['TAX_DETAIL_FILE'] = 'Irregular_Tax'
PATH['TAX_FILE'] = 'Irregular_Tax_Summary'
PATH['ATTACHMENT_FILE'] = '{0}_异常发票清单_{1}'
PATH['LOG_PATH'] = PATH['LOG_DIR'] + '\\' + get_job_name() + '.log'

DB['TAX_DETAIL_TABLE'] = 'Scrapy_' + PATH['SITE']
DB['TAX_INFO_TABLE'] = 'Scrapy_' + PATH['SITE'] + '_Summary'
DB['ACCESS_TABLE'] = 'Scrapy_Irregular_Tax_Access'


with ODBC(keys.dbconfig) as exist_db:
    access = exist_db.select(table_name=DB['ACCESS_TABLE'])
    condition = '[Timestamp] >= {0} AND [Source] = {1}'.format(get_sql_list(TIME['TODAY']), get_sql_list(PATH['SITE']))
    entities_done = exist_db.get_logs(condition=condition)
    # Exclude entities with logs in same day. If no logs, refresh table
    if entities_done:
        logger.info('Exclude existing entities and continue.')
        access_run = access[-access['Entity_Name'].isin(entities_done)]
    else:
        logger.info('Delete existing records and start a new query.')
        exist_db.delete(table_name=DB['TAX_INFO_TABLE'])
        exist_db.delete(table_name=DB['TAX_DETAIL_TABLE'])
        access_run = access

# Core scraping process
for index, row in access_run.iterrows():
    logger.info('---------------   Start new job. Entity: {} Server:{}    ---------------'.
                format(row['Entity_Name'], row['Server']))
    one_entity = PAM_Invoice(link=row['Link'], username=row['User_Name'], password=row['Password'])
    tax_df, tax_detail_df = one_entity.run(entity=row['Entity_Name'], server=row['Server'])

    # Upload to database
    entity_db = ODBC(keys.dbconfig)
    entity_db.upload(df=tax_df, table_name=DB['TAX_INFO_TABLE'])
    entity_db.upload(df=tax_detail_df, table_name=DB['TAX_DETAIL_TABLE'])
    entity_db.log(start=TIME['PRE3MONTH'], end=TIME['TODAY'], Timestamp=TIME['TIMESTAMP'], Source=PATH['SITE'],
                  Entity=row['Entity_Name'])
    entity_db.close()

# Ensure failure of scraping process do not interrupt email and sp execution
with ODBC(keys.dbconfig) as execute_db:
    # Update Irregular_Ind by executing stored procedure
    execute_db.call_sp(sp='CHN.Irregular_Tax_Refresh', table_name=DB['TAX_DETAIL_TABLE'],
                       table_name2=DB['TAX_DETAIL_TABLE'])
    for index, row in access.iterrows():
        # Get irregular record
        att = execute_db.call_sp(sp='CHN.Irregular_Tax_ETL', output=True, table_name=DB['TAX_DETAIL_TABLE'],
                                 entity_name=row['Entity_Name'])
        numeric_col = ['金额', '单价', '税率', '税额']

        if att is not None:
            att[numeric_col] = att[numeric_col].apply(pd.to_numeric)

        invoice_send_email(entity=row['Entity_Name'], receiver=row['Email_List'], attachment=att)

# Send email summary
scrapyemail_summary = Email()
scrapyemail_summary.send('[Scrapy]' + PATH['SITE'], 'Done', PATH['LOG_PATH'],
                         receivers='benson.chen@ap.jll.com;helen.hu@ap.jll.com')
scrapyemail_summary.close()
exit(0)
