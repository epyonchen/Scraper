"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""
import os
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import logging.config
from pytz import timezone
from func_timeout import func_timeout, FunctionTimedOut

# path
SCRIPT_DI = '1'
SCRIPT_DIR = os.path.dirname(__file__)
PIC_DIR = SCRIPT_DIR + r'\Vcode'
FILE_DIR = SCRIPT_DIR + r'\Result'
LOG_DIR = SCRIPT_DIR + r'\Log'
LOG_TABLE_NAME = 'Scrapy_Logs'

# time
TIMESTAMP = str(datetime.datetime.now(timezone('UTC')).astimezone(timezone('Asia/Hong_Kong')))
TODAY = date.today().strftime('%Y-%m-%d')
YESTERDAY = (date.today() - relativedelta(days=1)).strftime('%Y-%m-%d')
PRE3MONTH = (date.today() - relativedelta(months=4)).strftime('%Y-%m-%d')

# email
MAIL_HOST = 'outlook.office365.com'
MAIL_PORT = '587'

# global log variable
__log = None


# Return logger, display INFO level logs in console and record ERROR level logs in file
def getLogger(site):
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
        if __log is None:
            logging.config.dictConfig(LOGGING_CONFIG)
            __log = logging.getLogger('scrapy')
            return __log
        return __log


# Kill process if timeout
def timeout(func, time=3000, **kwargs):
    try:
        return func_timeout(timeout=time, func=func, kwargs=kwargs)
    except FunctionTimedOut as e:
        logger = logging.getLogger('scrapy')
        logger.error('Timeout:\n%s', e)
        exit(1)