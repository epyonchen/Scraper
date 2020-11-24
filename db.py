# -*- coding: utf-8 -*-
"""
Created on Jan 24th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pymssql
import pandas as pd
import pyodbc
from utility_commons import DB, get_df_col_size, chunksize_df_col_size, get_job_name
from utility_log import get_logger

logger = get_logger(__name__)


class DbHandler:
    def __init__(self, config):
        self.server = config['server']
        self.database = config['database']
        self.schema = config['schema']
        self.df = None
        self.conn = self._set_con(config)
        self.cur = self.conn.cursor()

    def __enter__(self):
        logger.info('Connect database: {}'.format(self.database))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.exception('{}, {}, {}'.format(exc_type, exc_val, exc_tb))
        self.close()

    # Create table
    def create_table(self, table_name, column_name, schema=None):
        if isinstance(column_name, list):
            columns_dict = {col: DB['DEFAULT_COL_SIZE'] for col in column_name}
        else:
            columns_dict = column_name

        columns = self._get_columns(columns_dict)
        table = self._get_table(table_name, schema)
        query = 'CREATE TABLE {0} ({1})'.format(table, columns)
        logger.info('Creat table {0}'.format(table))
        return self.run(query)

    # insert df into table
    def upload(self, df, table_name, schema=None, new_id=None, dedupe_col=None):
        self.df = chunksize_df_col_size(df)
        table = self._get_table(table_name, schema)
        column_list = list(self.df.columns.values)

        # Format column names with uid
        if new_id:
            column_list += ['UID']

        # Get input df's column dict
        column_dict = {col: DB['DEFAULT_COL_SIZE'] for col in column_list}
        column_dict.update(get_df_col_size(self.df))

        # If table does not exist, create one
        if not self.exist(table_name):
            logger.info('Table {} does not exist.'.format(table_name))
            if not self.create_table(table_name=table_name, column_name=column_dict, schema=schema):
                return None
        else:
            self.update_table_col_size(df=self.df, table_name=table_name, schema=schema)

        # Load to temp table
        values = []
        count = 0
        total = 0

        if not self.exist('#Temp_{}'.format(table_name)):
            self.create_table(table_name='#Temp_{}'.format(table_name), column_name=column_dict, schema=False)
        # Built insert query
        for index, row in self.df.iterrows():
            values.append(self._get_value(df_row=row, columns_order=column_list, new_id=new_id))

            count += 1
            total += 1
            if (count % 500 == 0) or (total >= len(self.df.index)):
                values = ','.join(values)
                temp_query = 'INSERT INTO #Temp_{} ({}) VALUES {}'. \
                    format(table_name, self._get_columns(column_list), values)
                logger.info('Insert {} rows'.format(total))
                if not self.run(temp_query):
                    return False
                values = []
                count = 0

        # Build deduplicate where condition
        if dedupe_col:
            self.drop_table_duplicate(dedupe_col=dedupe_col, df=self.df, table_name=table_name, schema=schema)

        # Columns cross-check
        existing_columns = list(self.select(table_name=table_name, column_name='top 0 *'))
        insert_columns = list(set(existing_columns) & set(column_list))
        insert_columns = self._get_columns(insert_columns)
        insert_query = 'INSERT INTO {0} ({1}) (SELECT {2} FROM #Temp_{3})'. \
            format(table, insert_columns, insert_columns, table_name)
        drop_temp = 'DROP TABLE #Temp_{0}'.format(table_name)

        if self.run(insert_query):
            logger.info('Total {0} records has been loaded into {1}'.format(total, table_name))
            self.run(drop_temp)
        else:
            logger.error('Fail to load data into {0}'.format(table_name))

    # Select all
    def select(self, table_name, column_name='*', schema=None, condition=''):
        if column_name != '*':
            columns = self._get_columns(column_name)
        else:
            columns = column_name
        if (not schema == 'INFORMATION_SCHEMA') and (not self.exist(table_name)):
            logger.error('Table {} does not exist.'.format(table_name))
            return None
        table = self._get_table(table_name, schema)

        # Condition build up
        if condition != '':
            condition = 'WHERE ' + condition

        query = "SELECT {} FROM {} {}".format(columns, table, condition)
        result = pd.read_sql(query, self.conn)
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
            logger.info('Execute store procedure: {}'.format(sp))

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
        except Exception:
            logger.exception('Fail to call sp')
            return False

    # Update one column of value with/without condition
    def update(self, table_name, set_col, set_value, set_case=True, schema=None, **kwargs):
        table = self._get_table(table_name, schema)
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
        if len(value) > 255:
            logger.error('Update value exceed length limition, please shorten into 255.')
            return None
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

    # Execute a DML query
    def run(self, query, output=False):
        try:
            self.cur.execute(query)
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
        except Exception:
            logger.exception('SQL execution failed. \n {}'.format(query))
            self.conn.rollback()
            return False

    # Delete a load
    def delete(self, table_name, schema=None, condition=''):
        if not self.exist(table_name):
            return False
        table = self._get_table(table_name, schema)
        # Condition build up
        if condition != '':
            condition = 'WHERE ' + condition
        query = 'DELETE FROM {} {}'.format(table, condition)
        logger.info('Delete record in {}, {}'.format(table, condition))
        self.run(query)

    # Close connection
    def close(self):
        logger.info('Disconnect database: {}'.format(self.database))
        self.conn.close()

    def log(self, log_table_name=DB['LOG_TABLE_NAME'], schema=None, **logs):
        job_name = get_job_name()
        log_df = pd.DataFrame([logs])
        self.upload(df=log_df, table_name=log_table_name, schema=schema, new_id=job_name)
        logger.info('Log current job.')

    # Set default schema
    def set_schema(self, new_schema):
        self.schema = new_schema

    # Get table's columns' size
    def get_table_col_size(self, table_name):
        col_info = self.select(table_name='COLUMNS', schema='INFORMATION_SCHEMA',
                               condition='TABLE_NAME = \'{0}\''.format(table_name))
        if (col_info is not None) and (not col_info.empty):
            col_info = col_info[['COLUMN_NAME', 'CHARACTER_MAXIMUM_LENGTH']].set_index('COLUMN_NAME')
            return col_info.to_dict()['CHARACTER_MAXIMUM_LENGTH']
        return None

    # Update column size between input df/target
    def update_table_col_size(self, df, table_name, schema=None):
        table_col_info = self.get_table_col_size(table_name)
        df_col_info = get_df_col_size(df)
        if table_col_info:
            common_cols = set((table_col_info.keys())) & set((df_col_info.keys()))
            for col in common_cols:
                if table_col_info[col] < df_col_info[col]:
                    x_col_info = DB['MAX_COL_SIZE'] if df_col_info[col] > DB['MAX_COL_SIZE'] else df_col_info[col]
                    query = 'ALTER TABLE {0} ALTER COLUMN {1} NVARCHAR({2})'. \
                        format(self._get_table(table_name, schema), self._get_columns(col), x_col_info)
                    logger.info('ALTER TABLE {0} COLUMN {1} TO NVARCHAR({2})'.format(table_name, col, x_col_info))
                    self.run(query, output=False)

    # Drop duplicate records in table in dedupe_col
    def drop_table_duplicate(self, dedupe_col, df, table_name, schema=None):
        table_df = self.select(table_name=table_name, schema=schema, column_name=self._get_columns(dedupe_col))
        common_columns = list(set(table_df.columns.values) & set(df.columns.values))
        dedupe_source = 'dedupe_source'
        dedupe_flag = 'dedupe_falg'
        if dedupe_source in common_columns:
            dedupe_source += '_bk'
        table_df[dedupe_source] = 'table'
        df[dedupe_source] = 'df'
        dedup_df = pd.concat([table_df, df], join='inner', axis=0, ignore_index=True)
        if dedupe_flag in common_columns:
            dedupe_flag += '_bk'
        dedup_df[dedupe_flag] = dedup_df.duplicated(subset=[dedupe_col], keep=False)
        dedup_df = dedup_df[((dedup_df[dedupe_source] == 'table') & dedup_df[dedupe_flag])]
        dedup_df = dedup_df[dedupe_col].drop_duplicates().reset_index()
        self.delete(table_name=table_name, schema=schema, condition='[{}] IN {}'.
                    format(dedupe_col, self._get_value(dedup_df[dedupe_col], dedup_df[dedupe_col].index)))

    # Format table if schema is not default, temp table when schema is False
    def _get_table(self, table_name, schema=None):
        if schema:
            table = '[{}].[{}]'.format(schema, table_name)
        elif schema is None:
            table = '[{}].[{}]'.format(self.schema, table_name)
        else:
            table = table_name
        return table

    # Get habdler's connection
    @staticmethod
    def _set_con(config):
        logger.error('No package.')
        return config

    # Format columns into brackets
    @staticmethod
    def _get_columns(column_name):
        if column_name:
            if isinstance(column_name, list):
                return ','.join(map(lambda x: '[{}]'.format(x), column_name))
            elif isinstance(column_name, dict):
                s = ''
                for k, v in column_name.items():
                    s += '[{0}] NVARCHAR({1}), '.format(k, v)
                return s
            else:
                return column_name

    # Get sql style insert value
    @staticmethod
    def _get_value(df_row, columns_order, new_id=None):
        def _format_value(v):
            return 'N\'{}\''.format(str(v).replace('\'', '\'\'') if not pd.isna(v) else '')

        df_row = df_row.apply(_format_value)
        if new_id:
            id_value = 'LEFT(\'{}_\' +  CONVERT(NVARCHAR(100), NEWID()),50)'.format(new_id)
            df_row['UID'] = id_value
        value_init = ','.join(df_row[columns_order].to_list())
        if 'N\'\'' in value_init:
            value_init = value_init.replace('N\'\',', 'NULL,')
        row_value = '({})'.format(value_init)

        return row_value


class Mssql(DbHandler):

    @staticmethod
    def _set_con(config):
        if ('username' in config.keys()) and ('password' in config.keys()):
            return pymssql.connect(server=config['server'], database=config['database'], user=config['username'],
                                   password=config['password'])
        else:
            return pymssql.connect(server=config['server'], database=config['database'])


class ODBC(DbHandler):

    @staticmethod
    def _set_con(config):

        driver = config['driver'] if 'driver' in config.keys() else 'SQL Server Native Client 11.0'
        con_str = 'DRIVER={0};SERVER={1};DATABASE={2};Trusted_Connection=yes;'. \
            format(driver, config['server'], config['database'])
        if ('username' in config.keys()) and ('password' in config.keys()):
            con_str += 'UID={0};PWD={1};'.format(config['username'], config['password'])
        return pyodbc.connect(con_str)

    # TODO: add bcp upload
    # TODO: rebuild log function


def get_sql_list(values):
    if isinstance(values, str):
        values = [values]

    values = map(lambda x: 'N\'{}\''.format(x), values)
    return '({0})'.format(', '.join(values))
