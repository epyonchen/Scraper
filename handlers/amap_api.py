# -*- coding: utf-8 -*-
"""
Created on June 24th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""


from handlers import keys
from handlers.default_api import default_api
from utils import get_logger, get_nested_value
from utils.utility_geocode import gcj02_to_wgs84


logger = get_logger(__name__)


class Amap(default_api):
    # Required api input, 'api type': [corresponding keys]
    _api_keys = {
        'text': ['keywords', 'types', 'city'],  # specific keyword
        'around': ['keywords', 'types', 'location', 'radius', 'city'],  # poi of round area, <lon, lat>
        'polygon': ['keywords', 'types', 'polygon', 'city'],  # poi of polygon area
        'detail': ['id']
    }

    # Default api parameters
    _default_kwargs = {
        'citylimit': True,
        'offset': '1',
        'output': 'JSON',
        'page': 0,
        'key': keys.amap['map_ak']
    }

    # Alternative keyword of parameters along with api class
    _alter_kwargs = {
        'sign': 'sig',
        'keyword': 'keywords',
        'page': 'page',
        'lat': 'lat',
        'lon': 'lon'}

    def __init__(self, api='text'):
        super().__init__(api)
        self.base = 'https://restapi.amap.com/v3/place/{}?'.format(api)

    def geocode_convert(self, output):
        if output:
            output['lon'], output['lat'] = str(output['location']).split(',')
            output['MapIT_lon'], output['MapIT_lat'] = gcj02_to_wgs84(float(output['lon']), float(output['lat']))
        return output

    # Query from input df
    def query(self, source_df, **kwargs):
        results = super(Amap, self).query(source_df=source_df, **kwargs)
        # if not results.empty:
        #     results[['lon', 'lat']] = results['location'].str.split(',', 1, expand=True)
        #     results[['MapIT_lon', 'MapIT_lat']] = results.apply(
        #         lambda x: self.geocode_convert(float(x['lon']), float(x['lat'])),
        #         axis=1)
        return results

    def _get_sign(self, query):
        query = ''
        for key, value in self.parameters.items():
            query = query + '&' + key + '=' + str(value)
        raw_str = query + keys.amap['map_sk']

        return self.get_md5(raw_str)

    # Check if response valid
    def validate_response(self, api_response):
        # Validate response
        if not api_response:
            logger.error('No response from api.')
            return None
        elif api_response['status'] != '1':
            logger.error('Response error, {}'.format(api_response))
        else:
            one_call = list()
            for result in api_response['pois']:
                flat_record = get_nested_value(result)
                one_call.append(flat_record)

            return one_call
