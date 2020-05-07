"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""
import os
import datetime
import pandas as pd
import logging
import logging.config
from datetime import date
from dateutil.relativedelta import relativedelta
from pytz import timezone
from openpyxl import load_workbook
from func_timeout import func_timeout, FunctionTimedOut

# path
SCRIPT_DIR = os.path.dirname(__file__)
TARGET_DIR = r'C:\Users\benson.chen\Desktop\Scraper'
PIC_DIR = TARGET_DIR + r'\Vcode'
FILE_DIR = TARGET_DIR + r'\Result'
LOG_DIR = TARGET_DIR + r'\Log'
LOG_TABLE_NAME = 'Scrapy_Logs'

# time
_NOW = datetime.datetime.now(timezone('UTC')).astimezone(timezone('Asia/Hong_Kong'))
TIMESTAMP = str(_NOW)
TODAY = _NOW.strftime('%Y-%m-%d')
YESTERDAY = (_NOW - relativedelta(days=1)).strftime('%Y-%m-%d')
PRE3MONTH = (_NOW - relativedelta(months=3)).strftime('%Y-%m-%d')

# email
MAIL_HOST = 'outlook.office365.com'
MAIL_PORT = '587'

# global log variable
__log = None


# Return logger, display INFO level logs in console and record ERROR level logs in file
def getLogger(site='scrapy'):
        # Logging config

        LOGGING_CONFIG = {
            'version': 1,  # required
            'disable_existing_loggers': True,  # this config overrides all other loggers
            'formatters': {
                'brief': {
                    'format': '%(asctime)s\t%(levelname)s: %(message)s'
                },
                'precise': {
                    'format': '%(asctime)s\t%(levelname)s - %(filename)s[line:%(lineno)s] - %(funcName)s(): %(message)s'
                },
            },
            'handlers': {
                'console': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'brief',
                    # 'encoding': 'utf-8'
                },
                'file': {
                    'level': 'DEBUG',
                    'class': 'logging.FileHandler',
                    'formatter': 'precise',
                    'filename': LOG_DIR + '\\' + site + '.log',
                    'mode': 'w',
                    'encoding': 'utf-8'
                },
            },
            'loggers': {
                'scrapy': {
                    'level': 'DEBUG',
                    'handlers': ['console', 'file'],
                    'propagate': False
                }
            }
        }

        global __log
        # if __log is None:
        logging.config.dictConfig(LOGGING_CONFIG)
        __log = logging.getLogger(site)
        return __log
        # else return
        # return __log


# Kill process if timeout
def timeout(func, time=3000, **kwargs):
    try:
        return func_timeout(timeout=time, func=func, kwargs=kwargs)
    except FunctionTimedOut as e:
        logger = logging.getLogger('scrapy')
        logger.error('Timeout:\n%s', e)
        exit(1)


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
def excel_to_df(filename, path=None):
    floder_dir = path if path else FILE_DIR

    try:
        df = pd.read_excel(floder_dir + r'\{}}.xlsx'.format(filename), sort=False)
    except Exception as e0:
        try:
            df = pd.read_excel(floder_dir + r'\{}}.xls'.format(filename), sort=False)
        except Exception as e:
            logging.info(e)
            return None

    return df


# Convert dataframe to excel
def df_to_excel(df, filename, sheetname=None, path=None):
    floder_dir = path if path else FILE_DIR
    try:
        writer = pd.ExcelWriter(floder_dir + r'\{}}.xlsx'.format(filename), engine='openpyxl')
        workbook = load_workbook(writer.path)
        writer.book = workbook
        df.to_excel(writer, index=False, header=True, sheet_name=sheetname)
        writer.save()
        writer.close()
    except Exception as e:
        logging.info(e)
        return None

    return True
