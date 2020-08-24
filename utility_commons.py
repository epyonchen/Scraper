"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import os
import sys
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


# global log variable
__default_logger = 'scrapy'
__log_map = dict()
__log_file_path = PATH['LOG_DIR'] + r'\{}.log'
__handlers = {

}
# Logging config
LOG_CONFIG = {
    'version': 1,  # required
    'disable_existing_loggers': True,  # this config overrides all other loggers
    'formatters': {
        'brief': {
            'format': '%(levelname)s: %(message)s'
        },
        'precise': {
            'format': '%(asctime)s %(levelname)s - %(filename)s[line:%(lineno)s]: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'precise',
            # 'encoding': 'utf-8'
        },
        # __default_logger: {
        #     'level': 'DEBUG',
        #     'class': 'logging.FileHandler',
        #     'formatter': 'precise',
        #     'filename': __log_file_path.format(__default_logger),
        #     'mode': 'w',
        #     'encoding': 'utf-8'
        # },
        # 'module': {
        #     'level': 'DEBUG',
        #     'class': 'logging.FileHandler',
        #     'formatter': 'precise',
        #     'filename': PATH['LOG_DIR'] + '\\' + logger_name + '.log',
        #     'mode': 'w',
        #     'encoding': 'utf-8'
        # },
    },
    'loggers': {
        # __default_logger: {
        #     'level': 'DEBUG',
        #     'handlers': [__default_logger],
        #     'propagate': True,
        # }
    }
}


# Return logger, display INFO level logs in console and record ERROR level logs in file
def getLogger(logger_name=__default_logger, isjob=True):

    if isjob:
        global __default_logger, __log_map
        __default_logger = _get_job_name()

        # Set job root logger
        if __default_logger not in __log_map:
            _update_log_config(__default_logger, __default_logger)
            logging.config.dictConfig(LOG_CONFIG)
            ans = logging.getLogger(__default_logger)
            __log_map[__default_logger] = ans
            __log_map['default'] = ans

        if logger_name != __default_logger:
            logger_name = __default_logger + '.' + logger_name
            _update_log_config(logger_name, __default_logger)
            logging.config.dictConfig(LOG_CONFIG)
            ans = logging.getLogger(logger_name)
            __log_map[logger_name] = ans
    else:
        logging.config.dictConfig(LOG_CONFIG)
        ans = logging.getLogger(logger_name)
        __log_map[logger_name] = ans

    return __log_map.get(logger_name)


# Update log config, adding
def _update_log_config(logger_name, default_logger):
    global LOG_CONFIG
    hanlder_config = {
        logger_name: {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'precise',
            'filename': __log_file_path.format(logger_name),
            'mode': 'w',
            'encoding': 'utf-8'
        }
    }

    # Root logger doesn't use console handler
    logger_config = {
        logger_name: {
            'level': 'DEBUG',
            'handlers': [logger_name, 'console'] if logger_name != default_logger else [logger_name],
            'propagate': True,
        }
    }
    global LOG_CONFIG
    LOG_CONFIG['handlers'].update(hanlder_config)
    LOG_CONFIG['loggers'].update(logger_config)


# Get current job name
def _get_job_name():
    import re
    module = sys.modules['__main__']
    name_pattern = re.compile(r'\w+\.py')
    name = re.search(name_pattern, str(module.__file__)).group(0)
    return name.replace(r'.py', '')


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
def excel_to_df(filename, sheet_name=None, path=None):
    folder_dir = path if path else PATH['TARGET_DIR']
    para = {'sort': False, 'dtype': str}
    if sheet_name:
        para.update({'sheet_name': sheet_name})
    try:
        df = pd.read_excel(folder_dir + r'\{}.xlsx'.format(filename), **para)
    except:
        # TODO: reset logger
        logging.info('{}.xlsx does not exist, try {}.xls'.format(filename, filename))
        try:
            df = pd.read_excel(folder_dir + r'\{}.xls'.format(filename), **para)
        except Exception:
            logging.exception('Fail to import excel to df')
            return None

    return df


# Convert dataframe to excel
def df_to_excel(df, filename, sheet_name='Results', path=None):
    file_path = (path if path else PATH['TARGET_DIR']) + r'\{}.xlsx'.format(filename)
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
        logging.exception('Fail to export {} to excel.'.format(filename))
        return None

    return True


