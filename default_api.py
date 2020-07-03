# -*- coding: utf-8 -*-
"""
Created on April 24th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


import pandas as pd
import requests
import hashlib
from urllib import parse
from utility_commons import getLogger

logger = getLogger('scrapy')


class default_api:
    _api_keys = {'api': []}

    _default_kwargs = {}

    _alter_kwargs = {'sign': 'sign',
                     'page': 'page',
                     'lat': 'lat',
                     'lon': 'lon'}

    def __init__(self, api='api'):
        self.base = None
        self.parameters = self._default_kwargs.copy()
        self.input_keys = self._api_keys[api].copy()

    # Update input parameters
    def update_parameters(self, source_row=None, **kwargs):
        parameters = self._default_kwargs.copy()
        source_input_keys = set(self.input_keys).intersection(list(source_row.keys())) \
            if source_row is not None else None

        if source_input_keys:
            for input_key in source_input_keys:
                parameters.update({input_key: str(source_row[input_key])})
        else:
            logger.error('Valid query keyword is missing in source.')
            return None
        if kwargs:
            parameters.update(kwargs)

        return parameters

    def _get_sign(self):
        return None

    # Call api
    def call_api(self, **kwargs):
        query = ''

        for key, value in kwargs.items():
            query = query + '&' + key + '=' + parse.quote_plus(str(value))
        query = self.base + query + '&' + self._alter_kwargs['sign'] + '=' + self._get_sign(query)
        try:
            response = requests.get(query).json()
            # time.sleep(random.randint(1, 2))
        except Exception as e:
            logger.error(e)
            return None

        return response

    # Query from input df
    def query(self, source_df, **kwargs):
        results = pd.DataFrame()
        # Update default input
        if kwargs:
            self._default_kwargs = self.update_parameters(**kwargs)

        # Iterate source
        for index, row in source_df.iterrows():
            self.parameters = self.update_parameters(row)
            if self.parameters:
                logger.info('Running index: {}'.format(index))
            else:
                return results

            # Call api
            while True:
                api_response = self.call_api(**self.parameters)
                one_call = self.validate_response(api_response)

                # Update source into response
                if one_call:
                    for call in one_call:
                        call.update(row.to_dict())
                    results = results.append(one_call, ignore_index=True, sort=False)
                else:
                    break

                # Update page input
                if 'page' in self._alter_kwargs.keys() \
                        and self.parameters[self._alter_kwargs['page']] > 0:
                    self.parameters[self._alter_kwargs['page']] += 1
                else:
                    break

        return results

    # Validate api response
    @staticmethod
    def validate_response(api_response):
        return api_response

    # Convert bd to wgs
    @staticmethod
    def geocode_convert(lat, lon):
        return lat, lon

    # Return md5 encode
    @staticmethod
    def get_md5(raw_sn):
        return hashlib.md5(str(raw_sn).encode('utf-8')).hexdigest()
