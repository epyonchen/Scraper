# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pymssql
import pandas as pd
import logging
import pyodbc
from utility_commons import LOG_TABLE_NAME


logger = logging.getLogger('scrapy')


class Mssql:

    def __init__(self, config, pkg='pymssql'):
        self.server = config['server']
        self.database = config['database']
        self.schema = config['schema']
        if pkg == 'pymssql':
            if ('user' in config.keys()) and ('password' in config.keys()):
                self.conn = pymssql.connect(server=self.server, database=self.database, user=config['user'], password=config['password'])
            else:
                self.conn = pymssql.connect(server=self.server, database=self.database)
            self.cur = self.conn.cursor()

        elif pkg == 'pyodbc':
            if 'driver' in config.keys():
                self.driver = config['driver']
            else:
                self.driver = 'SQL Server Native Client 11.0'
            if ('user' in config.keys()) and ('password' in config.keys()):
                self.conn = pyodbc.connect.connect('DRIVER={};SERVER={};DATABASE={};UID={};PWD={}'.format(self.driver, self.server, self.database, config['username'], config['password']))
            else:
                self.conn = pyodbc.connect.connect('DRIVER={};SERVER={};DATABASE={};Trusted_Connection=yes;'.format(self.driver, self.server, self.database))
            self.cur = self.conn.cursor()
        else:
            logger.error('Wrong package.')

    def __enter__(self):
        logger.info('Connect database: {}'.format(self.database))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.exception('{}, {}, {}'.format(exc_type, exc_val, exc_tb))
        logger.info('Disconnect database: {}'.format(self.database))
        self.close()

    # Create table
    def create_table(self, table_name, columns, schema=None):
        columns_init = columns.replace(',', ' NVARCHAR(255),') + ' NVARCHAR(255)'
        table = self.get_table(table_name, schema)
        query = 'CREATE TABLE {} ({})'.format(table, columns_init)
        logger.info('Creat table {}'.format(table))
        return self.run(query)

    # insert df into table
    def upload(self, df, table_name, new_id=True, dedup=False, dedup_id='Source_ID', start='1', end='0', schema=None, **logs):
        table = self.get_table(table_name, schema)

        temp_columns = list(df)
        columns = ' [' + '], ['.join(temp_columns) + ']'
        value_default = '({})'
        if new_id:
            temp_columns.append('UID')
            columns = '[UID],' + columns
            value_default = value_default.format('\'{}_\' +  CONVERT(NVARCHAR(100), NEWID())'.format(table_name) + ',{}')
        if bool(logs):
            temp_columns = temp_columns + list(logs.keys())
            columns = columns + ', [' + '], ['.join(logs.keys()) + ']'
            log_values = '{} ,N\'' + '\',N\''.join(logs.values()) + '\''
            value_default = value_default.format(log_values)

        # If table does not exist, create one
        # if not self.exist(table_name):
        #     logger.info('Table {} does not exist'.format(table_name))
        #     if not self.create_table(table_name=table_name, columns=columns):
        #         return False

        # Load to temp table
        values = None
        count = 0
        total = 0

        # if not self.exist('#Temp_{}'.format(table_name)):
        self.create_table(table_name='#Temp_{}'.format(table_name), columns=columns, schema=False)
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
                temp_query = 'INSERT INTO #Temp_{} ({}) VALUES {}'.format(table_name, columns, values)
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

        # Columns cross-check

        existing_columns = list(self.select(table_name=table_name, source=0))
        insert_columns = list(set(existing_columns) & set(temp_columns))
        insert_columns = '[' + '], ['.join(insert_columns) + ']'
        insert_query = 'INSERT INTO {} ({}) (SELECT {} FROM #Temp_{} {})'.format(table, insert_columns, insert_columns, table_name, where_cond)

        drop_temp = 'DROP TABLE #Temp_{}'.format(table_name)
        if self.run(insert_query):
            if start <= end:
                self.log(table_name, start, end, **logs)
        self.run(drop_temp)

    # Select all
    def select(self, table_name, columns='*', schema=None, **kwargs):
        if columns != '*':
            columns = ', '.join(columns)

        table = self.get_table(table_name, schema)

        # if not self.exist(table_name):
        #     return False

        # Condition build up
        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            cust_cond = []
            if 'customized' in kwargs.keys():
                customized = kwargs['customized']
                for key, value in customized.items():
                    cust_cond.append('[{}] {}'.format(key, value))
                cust_condition = 'AND '.join(cust_cond)
                del kwargs['customized']
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))

            condition = 'WHERE ' + ' AND '.join(cond) + (' AND ' + cust_condition) if bool(cust_cond) else ''
        query = "SELECT {} FROM {} {}".format(columns, table, condition)
        result = pd.read_sql(query, self.conn)
        # df = pd.DataFrame(result)
        return result

    # select function
    def select_pyodbc(self, table_name, columns='*', schema=None, **kwargs):
        if columns != '*':
            columns = ', '.join(columns)

        table = self.get_table(table_name, schema)
        # if not self.exist(table_name):
        #     return False

        # Condition build up
        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            cust_cond = []
            if 'customized' in kwargs.keys():
                customized = kwargs['customized']
                for key, value in customized.items():
                    cust_cond.append('[{}] {}'.format(key, value))
                cust_condition = 'AND '.join(cust_cond)
                del kwargs['customized']
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))

            condition = 'WHERE ' + ' AND '.join(cond) + (' AND ' + cust_condition) if bool(cust_cond) else ''
        query = "SELECT {} FROM {} {}".format(columns, table, condition)
        result = pd.read_sql(query, self.conn)
        # df = pd.DataFrame(result)
        return result

    # Call stored procedure
    def call_sp(self, sp, output=False, **kwargs):
        try:
            inputs = ''
            if bool(kwargs):
                inputs = []
                for key, value in kwargs.items():
                    inputs.append('@{} = N\'{}\''.format(key, value))
                inputs = ', '.join(inputs)
            query = "EXEC {} {}".format(sp, inputs)
            self.cur.execute(query)
            # self.conn.commit()
            logger.info('Execute store procedure: {}'.format(sp))
            #
            if output:
                col_names = [i[0] for i in self.cur.description]
                att = []
                for row in self.cur:
                    att.append(row)
                self.conn.commit()
                att = pd.DataFrame(att, columns=col_names)
                return att
            else:
                self.conn.commit()
                return True
        except Exception as e:
            logger.exception(e)
            return False

    # Update one column of value with/without condition
    def update(self, table_name, set_col, set_value, set_case=True, schema=None, **kwargs):
        table = self.get_table(table_name, schema)
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
        query = 'UPDATE {} SET {} = {} {}'.format(table, set_col, value, condition)
        logger.info('Update record in {}, {}'.format(table, condition))

        return self.run(query)

    # Check if table exists
    def exist(self, table_name):
        query = 'IF EXISTS (SELECT * FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE \'%{}\') ' \
                'BEGIN SELECT 1 ' \
                'END ' \
                'ELSE SELECT 0'.format(self.database, table_name)

        self.cur.execute(query)
        return self.cur.fetchone()[0]

    # Execute a DML query without response
    def run(self, query):
        try:
            self.cur.execute(query)
            self.conn.commit()
            return True
        except Exception as e:
            logger.exception('SQL exception: {}'.format(e))
            print(query)
            self.conn.rollback()
            # delete = 'DELETE * FROM {} WHERE SOURCE_NAME = {}'
            # delete_query = map(lambda x: delete.format(x, sourcename), table_names.values())
            # delete_query = '; '.join(delete_query)
            # cur.execute(delete_query)
            # conn.commit()
            return False

    # Delete a load
    def delete(self, table_name, schema=None, **kwargs):
        # if not self.exist(table_name):
        #     return False
        table = self.get_table(table_name, schema)
        if not bool(kwargs):
            condition = ''
        else:
            cond = []
            for key, value in kwargs.items():
                cond.append('[{}] = N\'{}\''.format(key, value))
            condition = 'WHERE ' + 'AND '.join(cond)
        query = 'DELETE FROM {} {}'.format(table, condition)
        logger.info('Delete record in {}, {}'.format(table, condition))
        self.run(query)

    # Close connection
    def close(self):
        self.conn.close()

    def log(self, table_name, start, end, schema=None, **logs):

        log_columns = '[UID], [Start], [End], [Table], [' + '], ['.join(logs.keys()) + ']'

        # if not self.exist(log_table):
        #     self.create_table(schema=schema, table_name=LOG_TABLE_NAME, columns=log_columns)
        log_values = 'N\'' + '\',N\''.join([start, end, table_name]) + '\', N\'' + '\',N\''.join(logs.values()) + '\''
        log_values = '(\'{}_\' +  CONVERT(NVARCHAR(100), NEWID()), {})'.format(LOG_TABLE_NAME, log_values)

        query = 'INSERT INTO {} ({}) VALUES {}'.format(self.get_table(schema=schema, table_name=LOG_TABLE_NAME), log_columns, log_values)

        if self.run(query):
            logger.info('Log current job.')

    # Format table if schema is not default
    def get_table(self, table_name, schema=None):
        if schema:
            table = '[{}].[{}]'.format(schema, table_name)
        elif schema is None:
            table = '[{}].[{}]'.format(self.schema, table_name)
        else:
            table = table_name
        return table

    def set_schema(self, new_schema):
        self.schema = new_schema

if __name__ == '__main__':
    import keys

