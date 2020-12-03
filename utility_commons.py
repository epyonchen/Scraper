"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import os
import sys
import re
import datetime
import pandas as pd
import logging
import logging.config
from dateutil.relativedelta import relativedelta
from pytz import timezone
from openpyxl import load_workbook

# sys.path.append(r'C:\Users\benson.chen\Credentials')

__TARGET_DIR = r'C:\Users\benson.chen\Desktop\Scraper'
# path
PATH = {
    'SCRIPT_DIR': os.path.dirname(__file__),
    'TARGET_DIR': __TARGET_DIR,
    'PIC_DIR': __TARGET_DIR + r'\Vcode',
    'FILE_DIR': __TARGET_DIR + r'\Result',
    'LOG_DIR': __TARGET_DIR + r'\Log',

}

# database
DB = {
    'MAX_COL_SIZE': 250,
    'DEFAULT_COL_SIZE': 50,
    'LOG_TABLE_NAME': 'Scrapy_Logs',
}


# time
__NOW = datetime.datetime.now(timezone('UTC')).astimezone(timezone('Asia/Hong_Kong'))
TIME = {
    'TIMESTAMP': str(__NOW),
    'TODAY': __NOW.strftime('%Y-%m-%d'),
    'YESTERDAY': (__NOW - relativedelta(days=1)).strftime('%Y-%m-%d'),
    'PRE3MONTH': (__NOW - relativedelta(months=3)).strftime('%Y-%m-%d'),
}


# email
MAIL = {
    'MAIL_HOST': 'outlook.office365.com',
    'MAIL_PORT': '587',
}


# Get nested dict
def get_nested_value(record):
    new_record = record.copy()
    for key, value in record.items():
        if isinstance(value, dict):
            inner_dict = get_nested_value(value)
            new_record.update(inner_dict)
            del new_record[key]
        else:
            continue
    return new_record


# Convert  excel to dataframe
def excel_to_df(file_name, sheet_name=None, path=None):
    folder_dir = path if path else PATH['TARGET_DIR']
    para = {'sort': False, 'dtype': str}
    if sheet_name:
        para.update({'sheet_name': sheet_name})
    try:
        df = pd.read_excel(folder_dir + r'\{}.xlsx'.format(file_name), **para)
    except:
        logging.info('{}.xlsx does not exist, try {}.xls'.format(file_name, file_name))
        try:
            df = pd.read_excel(folder_dir + r'\{}.xls'.format(file_name), **para)
            df = df.fillna(value='').replace(to_replace='^nan$', value='', regex=True)
        except Exception:
            logging.exception('Fail to import excel to df')
            return None

    return df


# Convert dataframe to excel
def df_to_excel(df, file_name, sheet_name='Results', path=None):
    file_path = (path if path else PATH['TARGET_DIR']) + r'\{}.xlsx'.format(file_name)
    try:
        if os.path.exists(file_path):
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                writer.book = load_workbook(writer.path)
                df.to_excel(writer, index=False, header=True, sheet_name=sheet_name)
                writer.save()
                writer.close()
        else:
            df.to_excel(file_path, index=False, header=True, sheet_name=sheet_name)
    except Exception:
        logging.exception('Fail to export {} to excel.'.format(file_name))
        return None

    return file_path


# Return dict of given df's columns' size
def get_df_col_size(df):
    col_dict = dict(
        (col, max(df[col].astype(str).apply(lambda x: len(x) if x is not None else 0).max(), DB['DEFAULT_COL_SIZE']))
        for col in df.columns.values)
    return col_dict


# Format string length of columns in dataframe
def chunksize_df_col_size(df, max_len=DB['MAX_COL_SIZE'], inplace=False):
    df_copy = df.copy() if not inplace else df
    col_dict = get_df_col_size(df_copy)
    for k, v in col_dict.items():
        if v > max_len:
            df_copy[k] = df_copy[k].astype(str).str.slice(0, max_len)
    return df_copy


# Get current job name
def get_job_name():
    module = sys.modules['__main__']
    name_pattern = re.compile(r'\w+\.py')
    name = re.search(name_pattern, str(module.__file__)).group(0)
    return name.replace(r'.py', '')