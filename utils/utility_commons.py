# -*- coding: utf-8 -*-
"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import datetime
from dateutil.relativedelta import relativedelta
import logging
import logging.config
from openpyxl import load_workbook
import os
import pandas as pd
from pytz import timezone
import re
import sys


__TARGET_DIR = r'C:\Users\benson.chen\Desktop\Scraper' # os.path.abspath(os.path.dirname(__file__) + os.path.sep + "..")
__SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__) + os.path.sep + "..")

# path
PATH = {
    'SCRIPT_DIR': __SCRIPT_DIR,
    'TARGET_DIR': __TARGET_DIR,
    'PIC_DIR': __TARGET_DIR + r'\Vcode',
    'FILE_DIR': __TARGET_DIR + r'\Result',
    'LOG_DIR': __TARGET_DIR + r'\Log',
    'JOB_DIR': __SCRIPT_DIR + r'\jobs'

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
    para = {'dtype': str}
    if sheet_name:
        para.update({'sheet_name': sheet_name})

    if os.path.isfile(folder_dir + r'\{}.xlsx'.format(file_name)):
        file_path = folder_dir + r'\{}.xlsx'.format(file_name)
        para['engine'] = 'openpyxl'
    elif os.path.isfile(folder_dir + r'\{}.xls'.format(file_name)):
        file_path = folder_dir + r'\{}.xls'.format(file_name)
    else:
        logging.error('{} does not exist.'.format(file_name))

    try:
        df = pd.read_excel(file_path, **para)
    except Exception:
        logging.exception('Fail to import excel to df')
        return None

    df = df.fillna(value='').replace(to_replace='^nan$', value='', regex=True)
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


# Check if geckodriver is installed, if not download one
def get_geckodriver():
    for p in sys.path:
        if os.path.isfile(p + '\\Scripts\\geckodriver.exe'):
            logging.info('geckodriver is installed.')
            return True

    logging.info('geockodrive is not installed, downloading...')
    from webdrivermanager import GeckoDriverManager
    gdd = GeckoDriverManager()
    try:
        gdd.download_and_install()
        logging.info('geckodriver is installed.')
    except Exception:
        logging.exception('Not able to install geckodirver.\n')


def renew_timestamp():
    global __NOW
    __NOW = datetime.datetime.now(timezone('UTC')).astimezone(timezone('Asia/Hong_Kong'))
    TIME['TIMESTAMP'] = str(__NOW)
    TIME['TODAY'] = __NOW.strftime('%Y-%m-%d')
