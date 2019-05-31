# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pymssql
import pandas as pd
import logging


logger = logging.getLogger('scrapy')


class Mssql:

    def __init__(self, config):
        self.server = config['server']
        self.database = config['database']
        self.schema = config['schema']
        if ('user' in config.keys()) and ('password' in config.keys()):
            self.conn = pymssql.connect(server=self.server, database=self.database, user=config['user'], password=config['password'])
        else:
            self.conn = pymssql.connect(server=self.server, database=self.database)
        self.cur = self.conn.cursor()

    def __enter__(self):
        logger.info('Connect database: {}'.format(self.database))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error('{}, {}, {}'.format(exc_type, exc_val, exc_tb))
        logger.info('Disconnect database: {}'.format(self.database))
        self.close()

    # Create table
    def create_table(self, table, columns):
        columns_init = columns.replace(',', ' NVARCHAR(255),') + ' NVARCHAR(255)'
        query = 'CREATE TABLE {} ({})'.format(table, columns_init)
        logger.info('Creat table {}'.format(table))
        return self.run(query)

    # insert df into table
    def upload(self, df, table, new_id=True, dedup=False, dedup_id='Source_ID', start='1', end='0', **logs):

        columns = list(df)
        columns = ' [' + '], ['.join(columns) + ']'
        value_default = '({})'
        if new_id:
            columns = '[UID],' + columns
            value_default = value_default.format('\'{}_\' +  CONVERT(NVARCHAR(100), NEWID())'.format(table) + ',{}')
        if bool(logs):
            columns = columns + ', [' + '], ['.join(logs.keys()) + ']'
            log_values = '{} ,N\'' + '\',N\''.join(logs.values()) + '\''
            value_default = value_default.format(log_values)

        # If table does not exist, create one
        if not self.exist(table):
            logger.info('Table [{}].[{}] does not exist'.format(self.schema, table))
            if not self.create_table('[{}].[{}]'.format(self.schema, table), columns):
                return False

        # Load to temp table
        values = None
        count = 0
        total = 0

        if not self.exist('#Temp_{}'.format(table)):
            self.create_table('#Temp_{}'.format(table), columns)
        # Built insert query
        for index, row in df.iterrows():
            # Replace ' as empty
            row = map(lambda x: str(x).replace('\'', ''), row)
            value = 'N\'' + '\',N\''.join(row).replace('nan', '') + '\''
            value = value_default.format(value)

            if values is None:
                values = value
            else:
                values = values + ', ' + value

            count += 1
            total += 1
            if (count % 500 == 0) or (total >= len(df.index)):
                temp_query = 'INSERT INTO #Temp_{} ({}) VALUES {}'.format(table, columns, values)
                # query = 'INSERT INTO [{}].[{}] ({}) VALUES {}'.format(self.schema, table, columns, values)

                # If error, delete all records related this load
                logger.info('Insert {} rows'.format(total))
                if not self.run(temp_query):

                    return False
                values = None
                count = 0

        # Build deduplicate where condition
        if dedup:
            where_cond = 'WHERE {} NOT IN (SELECT DISTINCT {} FROM [{}].[{}])'.format(dedup_id, dedup_id, self.schema, table)
        else:
            where_cond = ''
        insert_query = 'INSERT INTO [{}].[{}] ({}) (SELECT {} FROM #Temp_{} {})'.format(self.schema, table, columns, columns, table, where_cond)

        drop_temp = 'DROP TABLE #Temp_{}'.format(table)
        # query = 'SELECT {} FROM #Temp_{} WHERE Office_ID NOT IN (SELECT DISTINCT Office_ID FROM [{}].[{}])'.format(columns, table, self.schema, table)
        if self.run(insert_query):
            if start <= end:
                self.log(table, start, end, **logs)
        self.run(drop_temp)

    # Select all
    def select(self, table, columns='*', **kwargs):
        if columns != '*':
            columns = ', '.join(columns)

        if not self.exist(table):
            return False

        # Condition build up
        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            if 'customized' in kwargs.keys():
                customized = kwargs['customized']
                cust_cond = []
                for key, value in customized.items():
                    cust_cond.append('[{}] {}'.format(key, value))
                cust_condition = 'AND '.join(cust_cond)
                del kwargs['customized']
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))
            condition = 'WHERE ' + ' AND '.join(cond) + (' AND ' + cust_condition) if bool(cust_cond) else ''

        query = 'SELECT {} FROM [{}].[{}] {}'.format(columns, self.schema, table, condition)
        result = pd.read_sql(query, self.conn)
        # df = pd.DataFrame(result)
        return result

    # Call stored procedure
    def call_sp(self, sp, output=False, **kwargs):
        try:
            input = ''
            if bool(kwargs):
                input = []
                for key, value in kwargs.items():
                    input.append('@{} = N\'{}\''.format(key, value))
                input = ', '.join(input)
            query = "EXEC {} {}".format(sp, input)

            self.cur.execute(query)
            logger.info('Execute store procedure: {}'.format(sp))
            # self.conn.commit()
            if output:
                col_names = [i[0] for i in self.cur.description]
                att = []
                for row in self.cur:
                    att.append(row)
                att = pd.DataFrame(att, columns=col_names)
                return att
            else:
                return True
        except Exception as e:
            logger.error(e)
            return False

    # Update one column of value with/without condition
    def update(self, table, set_col, set_value, set_case=True, **kwargs):
        if not self.exist(table):
            return False

        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))
            condition = 'WHERE ' + 'AND '.join(cond)
        if set_case:
            value = 'N\'{}\''.format(set_value)
        else:
            value = set_value
        query = 'UPDATE [{}].[{}] SET {} = {} {}'.format(self.schema, table, set_col, value, condition)
        logger.info('Update record in {}, {}'.format(table, condition))

        return self.run(query)

    # Check if table exists
    def exist(self, table):
        query = 'IF EXISTS (SELECT * FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE \'%{}\') ' \
                'BEGIN SELECT 1 ' \
                'END ' \
                'ELSE SELECT 0'.format(self.database, table)

        self.cur.execute(query)
        return self.cur.fetchone()[0]

    # Execute a DML query without response
    def run(self, query):
        try:
            self.cur.execute(query)
            self.conn.commit()
            return True
        except pymssql.Error as e:
            logger.error('SQL Error: {}'.format(e))
            print(query)
            self.conn.rollback()
            # delete = 'DELETE * FROM {} WHERE SOURCE_NAME = {}'
            # delete_query = map(lambda x: delete.format(x, sourcename), tablenames.values())
            # delete_query = '; '.join(delete_query)
            # cur.execute(delete_query)
            # conn.commit()
            return False

    # Delete a load
    def delete(self, table, **kwargs):
        if not self.exist(table):
            return False

        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))
            condition = 'WHERE ' + 'AND '.join(cond)
        query = 'DELETE FROM [{}].[{}] {}'.format(self.schema, table, condition)
        logger.info('Delete record in {}, {}'.format(table, condition))
        self.run(query)

    # Close connection
    def close(self):
        self.conn.close()

    def log(self, table, start, end, **logs):

        log_columns = '[UID], [Start], [End], [Table], [' + '], ['.join(logs.keys()) + ']'
        log_table = 'Scrapy_Logs'

        if not self.exist(log_table):
            self.create_table('[{}].[{}]'.format(self.schema, log_table), log_columns)

        log_values = 'N\'' + '\',N\''.join([start, end, table]) + '\', N\'' + '\',N\''.join(logs.values()) + '\''
        log_values = '(\'{}_\' +  CONVERT(NVARCHAR(100), NEWID()), {})'.format(log_table, log_values)

        query = 'INSERT INTO [{}].[{}] ({}) VALUES {}'.format(self.schema, log_table, log_columns, log_values)

        if self.run(query):
            logger.info('Log current job.')


if __name__ == '__main__':
    import keys

    d = Mssql(keys.dbconfig_win)
    print('exist temp[', d.exist('{}'.format('Scrapy_Irregular_TAX')))
    d.run('EXEC CHN.Irregular_Tax_Refresh')
    d.close()
