# -*- coding: utf-8 -*-
"""
Created on May 11th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""


import os
import re
import requests
import os
import handlers.pagemanipulate as pm
import utils.utility_email as em
from func_timeout import func_set_timeout
from func_timeout.exceptions import FunctionTimedOut
from PIL import Image
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from handlers.baidu_api import Baidu_ocr
from utils.utility_log import get_logger
from utils.utility_commons import PATH, TIME, excel_to_df, df_to_excel




logger = get_logger(__name__)
count = 0


class PAM_Invoice:

    def __init__(self, link, username, password):

        self.base = link
        self.username = username
        self.password = password
        self.web = pm.Page(self.base, 'normal')
        self.web.driver.implicitly_wait(10)
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()
        self.df = dict()

    # Crop validation code pic from screen shot
    def get_vcode_pic(self):
        try:
            self.web.driver.save_screenshot(PATH['SCREENSHOT_PATH'])
            pic = self.web.driver.find_element_by_xpath('//*[@id="crcpic"]')
        except Exception:
            logger.exception('Unable to get validation code pic.')
            return False
        vcode_pic = Image.open(PATH['SCREENSHOT_PATH'])
        vcode_pic = vcode_pic.crop((pic.location['x'], pic.location['y'], pic.location['x'] + pic.size['width'],
                                    pic.location['y'] + pic.size['height']))
        return vcode_pic

    # Validation code is valid with 4 letters and probability > 0.7
    def vcode_validate(self, result, threshold=0.7):
        if not result:
            return None
        vcode = result['words_result'][0]['words'].replace(' ', '')
        prop = result['words_result'][0]['probability']['average']
        vcode = ''.join(re.findall(re.compile('[A-Za-z0-9]+'), vcode))
        if (len(vcode) == 4) and prop > threshold:
            return vcode
        else:
            return False

    # Not return validation code until get valid one
    def get_vcode(self):
        global count
        while True:
            if count % 10 == 0:
                logger.info('Try {} times.'.format(count))
            count += 1
            if count % 100 == 0:
                return None

            vpic = self.get_vcode_pic()
            if vpic:
                ocr = Baidu_ocr()
                ocr_result = ocr.ocr_api_call(vpic, PATH['VCODE_PATH'], bin_threshold=100, detect_direction='false',
                                              language_type='ENG', probability='true')
                if (not ocr_result) and (ocr.switch < 4):
                    continue
                elif (not ocr_result) and (ocr.switch >= 4):
                    exit(1)
                vcode = self.vcode_validate(ocr_result)
                if vcode:
                    logger.info('Get validation code "{}". Try to login.'.format(vcode))
                    return vcode
                else:
                    # logger.info('Cannot recognize validation code, try again.')
                    try:
                        self.web.driver.find_element_by_xpath('//*[@id="crcpic"]').click()
                    except Exception:
                        logger.exception('Unable to refresh validation code pic.')
                        return None
            else:
                return False

    # Login
    @func_set_timeout(timeout=3600, allowOverride=True)
    def login(self):
        while True:

            vcode = self.get_vcode()
            if vcode:
                try:
                    self.web.send(path='//*[@id="login"]/tbody/tr/td/table/tbody/tr[3]/td[1]/input',
                                  value=self.username)
                    self.web.send(path='//*[@id="login"]/tbody/tr/td/table/tbody/tr[4]/td/input', value=self.password)
                    self.web.send(path='//*[@id="login"]/tbody/tr/td/table/tbody/tr[5]/td[1]/input', value=vcode)
                    self.web.click(path='//*[@id="loginbt"]')
                except Exception:
                    logger.exception('Unable to input username/password/validation code.')
                    self.renew()
                    continue

                if '验证码错误' in self.web.driver.page_source:
                    logger.info('Validation code is incorrect, try again.')
                    continue
                else:
                    try:
                        element = WebDriverWait(self.web.driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/table/tbody/tr/td[1]'))
                        )
                    finally:
                        logger.info('Sucessfully login.')
                        return True
            else:
                self.renew()

    # Export excel with tax records
    def get(self, startdate=TIME['PRE3MONTH'], enddate=TIME['TODAY'], valid=''):
        logger.info('Query start date: {}, end date: {}, valid: {}'.format(str(startdate), str(enddate), str(valid)))
        tax_query = self.base + 'cxtj/getInvInfo.do?act=down&machineID=&type=&startDate={}&endDate={}&zfbz=' \
                                '&fpxz=&gfmc=&fpdm=&startFphm=&endFphm=&str_shuilv=0.04;0.06;0.10;0.09;0.11;0.13;' \
                                '0.16;0.17;0.03;0.05;0.015;9999&xsdjh=&bszt='.format(str(startdate), str(enddate))
        tax_detail_query = self.base + 'cxtj/getInvInfoMx.do?act=down&machineID=&type=&startDate={}&endDate={}&' \
                                       'zfbz={}&fpxz=&gfmc=&fpdm=&startFphm=&endFphm=&spmc=&spgg=&str_shuilv=0.04;' \
                                       '0.06;0.10;0.09;0.11;0.13;0.16;0.17;0.03;0.05;0.015;9999&' \
                                       'xsdjh='.format(str(startdate), str(enddate), str(valid))
        tax_flag = self.download_file(query=tax_query, file_name=PATH['TAX_FILE'])
        tax_detail_flag = self.download_file(query=tax_detail_query, file_name=PATH['TAX_DETAIL_FILE'])
        return tax_flag and tax_detail_flag

    # Delete previous query file and download a new one
    def download_file(self, query, file_name, file_dir=PATH['FILE_DIR']):
        file_path = file_dir + r'\{}.xls'.format(file_name)
        self.check_last_query(file_path)
        try:
            self.update_cookies()
            response = self.session.get(query)
            with open(file_path, 'wb') as writer:
                writer.write(response.content)
            logger.info('Download file {}.'.format(file_path))
            return True
        except Exception:
            logger.exception('Fail to download file {}'.format(file_path))
            return False

    # Check if file from previous exists, and delete it
    def check_last_query(self, path):
        if os.path.isfile(path):
            logger.info('Delete previous {}.'.format(path))
            os.remove(path)

    def update_cookies(self):
        self.cookies = self.web.get_requests_cookies()
        self.session.cookies.update(self.cookies)

    # Renew selenium and session
    def renew(self):
        logger.info('Renew browser and session.')
        self.web.renew(self.base)
        self.web.driver.implicitly_wait(10)
        self.session = requests.session()
        self.cookies = requests.cookies.RequestsCookieJar()

    @func_set_timeout(timeout=3600, allowOverride=True)
    def run(self, entity, server):
        # t = cls(link, username, password)

        while True:
            # Exit with error when login takes too much time
            try:
                self.login()
            except FunctionTimedOut as e:
                logger.exception('Timeout. {0}'.format(e))
                exit(1)
            except Exception as e:
                logger.exception(e)
            # self.login()

            success = self.get()
            if success:
                self.web.close()
                break
            else:
                logger.info('Restart job, Entity {} Server {}'.format(entity, server))
                self.renew()
                continue

        df = excel_to_df(path=PATH['FILE_DIR'], file_name=PATH['TAX_FILE'], sheet_name='发票信息')
        df.dropna(subset=['序号'], inplace=True)
        df['企业税号'] = entity
        df['服务器号'] = server

        detail_df = excel_to_df(path=PATH['FILE_DIR'], file_name=PATH['TAX_DETAIL_FILE'], sheet_name='商品信息')
        detail_df.dropna(subset=['序号'], inplace=True)
        detail_df['企业税号'] = entity
        detail_df['服务器号'] = server
        detail_df['timestamp'] = TIME['TIMESTAMP']

        return df, detail_df


def invoice_send_email(entity, receiver, attachment):
    # Send email
    scrapymail = em.Email()

    if (attachment is None) or attachment.empty:
        subject = '[PAM Tax Checking] - {0} 发票无异常 {1}'.format(TIME['TODAY'], entity)
        content = 'Hi All,\r\n\r\n{}的发票无异常记录。\r\n\r\nThanks.'.format(entity)
        scrapymail.send(subject=subject, content=content, receivers=receiver, attachment=None)
    else:
        subject = '[PAM Tax Checking] - {0} 发票异常清单 {1}'.format(TIME['TODAY'], entity)
        content = 'Hi All,\r\n\r\n请查看附件关于{}的发票异常记录。\r\n\r\nThanks.'.format(entity)
        entity_path = df_to_excel(df=attachment, path=PATH['FILE_DIR'],
                    file_name=PATH['ATTACHMENT_FILE'].format(TIME['TODAY'], entity), sheet_name=entity)
        scrapymail.send(subject=subject, content=content, receivers=receiver, attachment=entity_path)
        logger.info('Delete attachment file.')
        os.remove(entity_path)

    scrapymail.close()

