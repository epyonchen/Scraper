# -*- coding: utf-8 -*-

import pandas as pd
import keys
import requests
import time
import random
import hashlib
import urllib
import gecodeconvert as gc
from aip import AipOcr
from utility_commons import *

logger = logging.getLogger('scrapy')


class Baidu:

    def __init__(self, api=None):
        self.switch = 0
        if api == 'map':
            self.host = 'http://api.map.baidu.com'
            self.base = '/place/v2/search?'
        elif api == 'ocr':
            self.client = AipOcr(keys.baidu['ocr_id'], keys.baidu['ocr_ak'], keys.baidu['ocr_sk'])
        elif api == 'translate':
            self.base = 'http://api.fanyi.baidu.com/api/trans/vip/translate?'

    # def get_token(self, ak=keys.baidu['ocr_ak'], sk=keys.baidu['ocr_sk']):
    #     query_token = self.base_token.format(ak, sk)
    #     print(query_token)
    #     response = requests.get(query_token).json()
    #
    #     return response['access_token']

    def ocr_image_process(self, image, path, bin_threshold):
        img = image.convert('L')
        img = img.point(lambda p: p > bin_threshold and 255)
        img.save(path)
        with open(path, 'rb') as img_file:
            return img_file.read()

    def ocr_api_call(self, image, path, bin_threshold=100, **kwargs):
        bin_image = self.ocr_image_process(image, path, bin_threshold)
        try:
            result = self.client.basicGeneral(bin_image, kwargs)
        except Exception as e:
            self.renew_client_ocr()
            logger.exception(e)
            return False

        if ('words_result_num' in result.keys()) and result['words_result_num'] > 0:
            return result
        else:
            return False

    def renew_client_ocr(self):
        self.client = AipOcr(keys.baidu['ocr_id'], keys.baidu['ocr_ak'], keys.baidu['ocr_sk'])
        self.switch += 1

    # Call map api, refer parameter to http://lbsyun.baidu.com/index.php?title=webapi/guide/webservice-placeapi
    def map_api_call(self, map_ak, map_sk, **kwargs):
        def _get_sn(querystr, sk):
            encoded_str = urllib.parse.quote(querystr, safe="/:=&?#+!$,;'@()*[]")
            raw_str = urllib.parse.quote_plus(encoded_str + sk).encode('utf-8')

            return hashlib.md5(raw_str).hexdigest()

        query = self.base
        for key, value in kwargs.items():
            query = query + '&' + key + '=' + str(value)
        query = query + '&ak=' + map_ak
        sn = _get_sn(query, map_sk)
        query = self.host + query + '&sn=' + sn

        # Request api
        response = requests.get(query).json()
        time.sleep(random.randint(1, 2))

        # Validate response
        if response['status'] != 0:
            return False
        else:
            one_call = list()
            for record in response['results']:
                mapit = self.geocode_convert(record['location']['lat'], record['location']['lng'])
                record['MapITlat'] = mapit[1]
                record['MapITlon'] = mapit[0]
                record = self.get_nested_value(record)
                record['keyword'] = kwargs['query']

                # inflag = self.validate_in(store, location_input)
                # if inflag:
                #     df = df.append(store, ignore_index=True)
                # else:
                #     continue

                one_call.append(record)

            return one_call

    # Get nested dict
    def get_nested_value(self, record):
        new_record = record.copy()
        for key, value in record.items():
            if isinstance(value, dict):
                inner_dict = self.get_nested_value(value)
                new_record.update(inner_dict)
                del new_record[key]
            else:
                continue
        return new_record

    # Query with keywords list and cities list
    def search_location(self, keyword_inputs, city_inputs, location_input, **kwargs):
        df = pd.DataFrame()

        for city in city_inputs:
            for keyword in keyword_inputs:
                page = 1
                print('df size:', len(df))
                while page > 0:
                    api_parameter = kwargs.copy()
                    api_parameter.update({'query':keyword, 'region':city, 'page_num':(page-1)})
                    one_call = self.map_api_call(keys.baidu['map_ak'], keys.baidu['map_sk'], **api_parameter)
                    if one_call is not None:
                        df.append(one_call, ignore_index=True)
        return df

    # Convert from Baidu to wgs84
    def geocode_convert(self, lat, lon):

        return gc.bd09_to_wgs84(lon, lat)

    # Validate if location in return records
    def validate_in(self, record, location_in):
        for loc in location_in:
            if (loc in record['address']) or (loc in record['name']):
                return True
        return False

    # Call translate api, refer parameter to http://api.fanyi.baidu.com/api/trans/product/apidoc
    def translate_api_call(self, translate_id, translate_sk, keyword, from_lg, to_llg):
        salt = str(random.randint(32768, 65536))
        def _get_sign(id, sk, salt, q):
            raw_sn = id + q + salt + sk
            return hashlib.md5(raw_sn.encode('utf-8')).hexdigest()

        sign = _get_sign(translate_id, translate_sk, salt, keyword)
        parameter = 'appid={}&q={}&from={}&to={}&salt={}&sign={}'.format(translate_id, keyword, from_lg, to_llg, salt, sign)
        query = self.base + parameter

        try:
            response = requests.get(query).json()
            time.sleep(random.randint(1, 2))
        except Exception as e:
            logger.error(e)
            return None

        return response

    def translate(self, keyword_input, from_language='auto', to_language='en'):
        one_call = self.translate_api_call(keys.baidu['translate_id'], keys.baidu['translate_sk'],
                                           keyword=keyword_input, from_lg=from_language, to_llg=to_language)
        if one_call:
            if len(one_call['trans_result']) > 0:
                return one_call['trans_result'][0]
            else:
                return None

if __name__ == '__main__':
    a = Baidu(api='translate')

    print(a.translate('周大福金融中心'))

