logger.info('---------------   Irregular tax ratio query.   ---------------')

with Mssql(keys.dbconfig) as exist_db:
    access = exist_db.select(DB['ACCESS_TABLE'])
    condition = '[Timestamp] >= {0} AND [Source] = {1}'. \
        format(get_sql_list(TIME['TODAY']), get_sql_list(SITE))
    entities = '\'' + '\', \''.join(list(access['Entity_Name'])) + '\''
    logs = exist_db.select(table_name=DB['LOG_TABLE_NAME'], condition=condition)
    # Exclude entities with logs in same day. If no logs, refresh table
    if not logs.empty:
        logger.info('Exclude existing entities and continue.')
        access_run = access[-access['Entity_Name'].isin(logs['Entity'])]
    else:
        logger.info('Delete existing records and start a new query.')
        exist_db.delete(table_name=DB['TAX_TABLE'])
        exist_db.delete(table_name=DB['TAX_DETAIL_TABLE'])
        access_run = access

# Core scraping process
for index, row in access_run.iterrows():
    logger.info('---------------   Start new job. Entity: {} Server:{}    ---------------'.
                format(row['Entity_Name'], row['Server']))
    one_entity = Tax(link=row['Link'], username=row['User_Name'], password=row['Password'])
    tax_df, tax_detail_df = one_entity.run(entity=row['Entity_Name'], server=row['Server'])

    # Upload to database
    entity_db = Mssql(keys.dbconfig)
    entity_db.upload(df=tax_df, table_name=DB['TAX_TABLE'])
    entity_db.upload(df=tax_detail_df, table_name=DB['TAX_DETAIL_TABLE'])
    entity_db.log(start=TIME['PRE3MONTH'], end=TIME['TODAY'], Timestamp=TIME['TIMESTAMP'], Source=SITE,
                  Entity=row['Entity_Name'])
    entity_db.close()

# Ensure failure of scraping process do not interrupt email and sp execution
with Mssql(keys.dbconfig) as execute_db:
    # Update Irregular_Ind by executing stored procedure
    execute_db.call_sp(sp='CHN.Irregular_Tax_Refresh', table_name=DB['TAX_DETAIL_TABLE'],
                       table_name2=DB['TAX_TABLE'])
    for index, row in access.iterrows():
        # Get irregular record
        att = execute_db.call_sp(sp='CHN.Irregular_Tax_ETL', output=True, table_name=DB['TAX_DETAIL_TABLE'],
                                 entity_name=row['Entity_Name'])
        numeric_col = ['金额', '单价', '税率', '税额']

        if att is not False:
            att[numeric_col] = att[numeric_col].apply(pd.to_numeric)

        _send_email(entity=row['Entity_Name'], receiver=row['Email_List'], attachment=att)

# Send email summary
scrapyemail_summary = em.Email()
scrapyemail_summary.send('[Scrapy]' + SITE, 'Done', PATH['LOG_PATH'],
                         receivers='benson.chen@ap.jll.com;helen.hu@ap.jll.com')
scrapyemail_summary.close()
exit(0)
