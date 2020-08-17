# -*- coding: utf-8 -*-
"""
Created on June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""


import random
import time
import urllib
import geocodeconvert as gc
from aip import AipOcr
from default_api import default_api
from utility_commons import getLogger, get_nested_value
import keys

logger = getLogger('scrapy')


class Baidu_map(default_api):
    # Required api input, 'api type': [corresponding keys]
    _api_keys = {
        'place': ['query', 'tag', 'region'],  # specific keyword
        'around': ['query', 'tag', 'location', 'radius'],  # poi of round area, <lat, lon>
        'polygon': ['query', 'tag', 'bounds', 'city'],  # poi of polygon area, <lat, lon>
        'detail': ['uid', 'uids']
    }

    # Default api parameters
    _default_kwargs = {
        'city_limit': 'true',
        'scope': '1',  # 1-basic return, 2-detail return
        'output': 'json',
        'coord_type': '3',  # 1-wgs, 3-bd
        'page_size': '1',
        'page_num': 0,
        'timestamp': str(round(time.time())),
        'ak': keys.baidu['map_ak']
    }

    # Alternative keyword of parameters along with api class
    _alter_kwargs = {
        'sign': 'sn',
        'keyword': 'query',
        'page': 'page_num',
        'lat': 'lat',
        'lon': 'lng'
    }

    # Convert bd to wgs
    @staticmethod
    def geocode_convert(lon, lat):
        return pd.Series(gc.bd09_to_wgs84(lon, lat))

    # Validate if location in return records
    @staticmethod
    def validate_in(record, location_in):
        for loc in location_in:
            if (loc in record['address']) or (loc in record['name']):
                return True
        return False

    def __init__(self, api='place'):
        super(Baidu_map, self).__init__(api)
        self.base = 'http://api.map.baidu.com/place/v2/search?'
        self.pre_query = '/place/v2/search?'
        # self.input_keys = self._api_keys[api].copy()

    def _get_sign(self, query):
        query = self.pre_query + query
        raw_str = urllib.parse.quote(query, safe="/:=&?#+!$,;'@()*[]") + keys.baidu['map_sk']
        return self.get_md5(urllib.parse.quote_plus(raw_str))

    # Query from input df
    def query(self, source_df, **kwargs):
        results = super(Baidu_map, self).query(source_df=source_df, **kwargs)
        if not results.empty:
            results[['MapIT_lon', 'MapIT_lat']] = results.apply(
                lambda x: self.geocode_convert(float(x['lng']), float(x['lat'])),
                axis=1)

        return results

    # Check if response valid
    def validate_response(self, api_response):
        # Validate response
        if not api_response:
            logger.error('No response from api.')
            return None
        elif str(api_response['status']) != '0':
            logger.error('Response error, status: {}'.format(api_response['status']))
        else:
            one_call = list()
            for result in api_response['results']:
                flat_record = get_nested_value(result)
                one_call.append(flat_record)

            return one_call


class Baidu_translate(default_api):
    # Required api input
    _api_keys = {'translate': ['q', 'from', 'to'],
                 }

    # Default api parameters
    _default_kwargs = {'appid': keys.baidu['translate_id'],
                       'salt': str(random.randint(32768, 65536)),
                       'from': 'auto',
                       'to': 'en',
                       }
    # Alternative keyword of parameters along with api class
    _alter_kwargs = {'sign': 'sign',
                     'keyword': 'q',
                     }

    def __init__(self, api='translate'):
        super().__init__(api)
        self.base = 'https://fanyi-api.baidu.com/api/trans/vip/translate?'

    def _get_sign(self, q):
        raw_sn = self.parameters['appid'] + self.parameters[self._alter_kwargs['keyword']] + self.parameters['salt'] \
                 + keys.baidu['translate_sk']
        return self.get_md5(raw_sn)

    # Check if response valid
    def validate_response(self, api_response):

        if not api_response:
            logger.error('No response from api.')
            return None
        elif 'error_code' in api_response.keys():
            logger.error('Response error code: {}'.format(api_response['error_code']))
        elif ('trans_result' in api_response.keys()) and (len(api_response['trans_result']) > 0):
            one_call = list()
            for result in api_response['trans_result']:
                flat_record = get_nested_value(result)
                flat_record.update(api_response)
                del flat_record['trans_result']
                one_call.append(flat_record)
                break
            return one_call


class Baidu_ocr(default_api):
    def __init__(self):
        self.switch = 0
        self.client = AipOcr(keys.baidu['ocr_id'], keys.baidu['ocr_ak'], keys.baidu['ocr_sk'])

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
        except Exception:
            self.renew_client_ocr()
            logger.exception('OCR api query fail')
            return False

        if ('words_result_num' in result.keys()) and result['words_result_num'] > 0:
            return result
        else:
            return False

    def renew_client_ocr(self):
        self.client = AipOcr(keys.baidu['ocr_id'], keys.baidu['ocr_ak'], keys.baidu['ocr_sk'])
        self.switch += 1


if __name__ == '__main__':
    from utility_commons import TARGET_DIR
    import pandas as pd
    df = pd.DataFrame()
    df = df.append([{'q': '', 'from': 'cn', 'to': 'en'}], ignore_index=True)
    print(df)
    bm = Baidu_translate()
    r = bm.query(df)
    print(r)
