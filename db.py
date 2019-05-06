# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pymssql
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Mssql:

    def __init__(self, config):
        self.server = config['server']
        self.database = config['database']
        self.user = config['user']
        self.password = config['password']
        self.schema = config['schema']
        self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
        self.cur = self.conn.cursor()
        logging.info('Connect to database: {}'.format(self.database))

    # Create table
    def create_table(self, table, columns):
        # if self.exist(table) is True:
        #     print('Table ')

        columns_init = columns.replace(',', ' NVARCHAR(255),') + ' NVARCHAR(255)'
        query = 'CREATE TABLE {} ({})'.format(table, columns_init)
        logging.info('Creat table {}'.format(table))
        # logging.info('Create table {} successfully.'.format(table))
        return self.run(query)

    # insert df into table
    def upload(self, load_list, table, *start_end, **logs):
        # load_list = load_list[colnames]
        columns = list(load_list)
        columns = '[UID],' + ' [' + '], ['.join(columns) + '], [' + '], ['.join(logs.keys()) + ']'
        log_values = '\',N\''.join(logs.values())

        # If table does not exist, create one
        if not self.exist(table):
            logging.info('Table [{}].[{}] does not exist'.format(self.schema, table))
            if not self.create_table('[{}].[{}]'.format(self.schema, table), columns):
                return False

        # Load to temp table
        values = None
        count = 0
        total = 0
        if not self.exist('#Temp_{}'.format(table)):
            self.create_table('#Temp_{}'.format(table), columns)
        for index, row in load_list.iterrows():
            row = map(lambda x: str(x).replace('\'', ''), row)
            value = '\',N\''.join(row).replace('nan', '')
            value = '(\'{}_\' +  CONVERT(NVARCHAR(100), NEWID()), N\'{}\', N\'{}\')'.format(table, value, log_values)
            if values is None:
                values = value
            else:
                values = values + ', ' + value

            count += 1
            total += 1
            if (count % 500 == 0) or (total >= len(load_list.index)):
                temp_query = 'INSERT INTO #Temp_{} ({}) VALUES {}'.format(table, columns, values)
                # query = 'INSERT INTO [{}].[{}] ({}) VALUES {}'.format(self.schema, table, columns, values)

                # If error, delete all records related this load
                logging.info('Insert {} rows'.format(total))
                if not self.run(temp_query):
                    # self.delete_load(table, kwargs)
                    return False
                values = None
                count = 0

        # Load to table
        insert_query = 'INSERT INTO [{}].[{}] ({}) (SELECT {} FROM #Temp_{} WHERE Source_ID NOT IN (SELECT DISTINCT Source_ID FROM [{}].[{}]))'.format(self.schema, table, columns, columns, table, self.schema, table)
        drop_temp = 'DROP TABLE #Temp_{}'.format(table)
        # query = 'SELECT {} FROM #Temp_{} WHERE Office_ID NOT IN (SELECT DISTINCT Office_ID FROM [{}].[{}])'.format(columns, table, self.schema, table)
        if self.run(insert_query):
            self.log(table, *start_end, **logs)
        self.run(drop_temp)


    # Select all
    def get_all(self, table, columns='*'):
        if columns != '*':
            columns = ', '.join(columns)
        query = 'SELECT {} FROM [{}].[{}]'.format(columns, self.schema, table)
        print(query)
        result = pd.read_sql(query, self.conn)
        df = pd.DataFrame(result)

        return df

    # Select one
    def get_one(self, table, column=None, value=None):
        condition = None
        if (column is not None) and (value is not None):
            condition = 'WHERE {} = N\'{}\''.format(column, value)
        query = 'SELECT * FROM [{}].[{}] {}'.format(self.schema, table, condition)
        result = pd.read_sql(query, self.conn)
        df = pd.DataFrame(result)

        return df

    # Update one column of value with/without condition
    def update_one(self, table, set_col, set_value, **kwargs):
        condition = 'WHERE 1 '
        for key, value in kwargs:
            condition = condition + 'AND {} = \'{}\''.format(key, value)
        query = 'UPDATE [{}].[{}] SET {} = N\'{}\' {}'.format(self.schema, table, set_col, set_value, condition)
        logging.info('Update record in {}, {}'.format(table, condition))

        return self.run(query)

    # Check if table exists
    def exist(self, table):
        query = 'IF EXISTS (SELECT * FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE \'%{}%\') ' \
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
            logging.error('SQL Error: {}'.format(e))
            print(query)
            self.conn.rollback()
            # delete = 'DELETE * FROM {} WHERE SOURCE_NAME = {}'
            # delete_query = map(lambda x: delete.format(x, sourcename), tablenames.values())
            # delete_query = '; '.join(delete_query)
            # cur.execute(delete_query)
            # conn.commit()
            return False

    # Delete a load
    def delete_load(self, table, **kwargs):
        condition = 'WHERE 1 '
        for key, value in kwargs:
            condition = condition + 'AND [{}] = N\'{}\''.format(key, value)
        query = 'DELETE FROM [{}].[{}] {}'.format(self.schema, table, condition)
        logging.info('Delete record in {}, {}'.format(table, condition))
        return self.run(query)

    # Close connection
    def close(self):
        self.conn.close()

    def log(self, table, *start_end, **logs):

        log_columns = '[UID], [Start], [End], [Table], [' + '], ['.join(logs.keys()) + ']'
        log_table = 'Scrapy_Logs'

        if not self.exist(log_table):
            self.create_table('[{}].[{}]'.format(self.schema, log_table), log_columns)

        log_values = 'N\'' + '\',N\''.join(list(start_end) + [table]) + '\', N\'' + '\',N\''.join(logs.values()) + '\''
        log_values = '(\'{}_\' +  CONVERT(NVARCHAR(100), NEWID()), {})'.format(log_table, log_values)

        query = 'INSERT INTO [{}].[{}] ({}) VALUES {}'.format(self.schema, log_table, log_columns, log_values)

        self.run(query)
        logging.info('Log current job.')


if __name__ == '__main__':
    import keys
    d = Mssql(keys.dbconfig)
    d.log('1', '2', a='1')
