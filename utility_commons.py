"""
Created on Thur May 16th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""
import os
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
from pytz import timezone

# path
SCRIPT_DI = '1'
SCRIPT_DIR = os.path.dirname(__file__)
PIC_DIR = SCRIPT_DIR + r'\Vcode'
FILE_DIR = SCRIPT_DIR + r'\Result'

# time
TIMESTAMP = str(datetime.datetime.now(timezone('UTC')).astimezone(timezone('Asia/Hong_Kong')))
TODAY = date.today().strftime('%Y-%m-%d')
YESTERDAY = (date.today() - relativedelta(days=1)).strftime('%Y-%m-%d')
PRE3MONTH = (date.today() - relativedelta(months=4)).strftime('%Y-%m-%d')

# email
MAIL_HOST = 'outlook.office365.com'
MAIL_PORT = '587'

# TODO: logging module
class Log:
    def __init__(self):
        return logging.getLogger('')