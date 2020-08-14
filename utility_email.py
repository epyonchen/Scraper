# -*- coding: utf-8 -*-
"""
Created on June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""


import smtplib
import re
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.header import Header
from utility_commons import MAIL, getLogger
import keys

logger = getLogger(__name__)


class Email:
    def __init__(self, username=keys.email['username'], password=keys.email['password'],
                 host=MAIL['MAIL_HOST'], port=MAIL['MAIL_PORT']):
        self.smtpObj = smtplib.SMTP(host)
        self.smtpObj.connect(host, port)
        self.smtpObj.starttls()
        self.smtpObj.ehlo()
        self.smtpObj.login(username, password)
        self.msg = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error('{}, {}, {}'.format(exc_type, exc_val, exc_tb))
        self.close()

    def send(self, subject, content, attachment=None, sender='TDIM.China@ap.jll.com', receivers='benson.chen@ap.jll.com'):
        if isinstance(receivers, list):
            receivers_list = receivers
            receivers = '; '.join(receivers)
        elif isinstance(receivers, str):
            receivers_list = receivers.split(';')
        else:
            logger.error('Receivers must be string value.')
            return None

        if not self.check_connection():
            self.reconnect()

        try:
            self.smtpObj.sendmail(sender, receivers_list, self.build_msg(subject, content, attachment, sender, receivers))
            logger.info('Email has been sent to {}'.format(receivers))
            return True
        except Exception:
            logger.exception('Fail to send email')
            return None

    def reconnect(self, username=keys.email['username'], password=keys.email['password'],
                  host=MAIL['MAIL_HOST'], port=MAIL['MAIL_PORT']):
        logger.info('Reconnect mailing server')
        self.smtpObj = smtplib.SMTP()
        self.smtpObj.connect(host, port)
        self.smtpObj.starttls()
        self.smtpObj.ehlo()
        self.smtpObj.login(username, password)

    def check_connection(self):
        try:
            status = self.smtpObj.noop()[0]
        except Exception:
            logger.exception('Fail to connect email server.')
            status = -1
        return status == 250

    def close(self):
        self.smtpObj.quit()

    def build_msg(self, subject, content, attachment=None,
                  sender='TDIM.China@ap.jll.com', receivers='benson.chen@ap.jll.com'):
        self.msg = MIMEMultipart()
        self.msg.attach(MIMEText(content, 'plain', 'utf-8'))
        self.msg['Subject'] = Header(subject, 'utf-8')
        self.msg['From'] = sender
        self.msg['To'] = receivers
        if attachment:
            att = MIMEApplication(open(attachment, 'rb').read())
            att.add_header('Content-Disposition', 'attachment',
                           filename=re.compile(r'((?!\\).)*[A-Za-z0-9]((?!\\).)*$').search(attachment).group(0))
            self.msg.attach(att)

        return self.msg.as_string()
