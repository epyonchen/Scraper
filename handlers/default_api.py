# -*- coding: utf-8 -*-
"""
Created on April 24th 2020

@author: Benson.Chen benson.chen@ap.jll.com
"""


import hashlib
import requests
import pandas as pd
from urllib import parse
from utils import get_logger


logger = get_logger(__name__)


class default_api:
    # Required api input, 'api type': [corresponding keys]
    _api_keys = {'api': []}

    # Default api parameters
    _default_kwargs = {}

    # Alternative keyword of parameters along with api class
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
        # Check if required keywords in inputs
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

    # Call api, parse input parameters to api query, send query and get response back.
    # Return response as json
    def call_api(self, **kwargs):
        query = ''

        for key, value in kwargs.items():
            query = query + '&' + key + '=' + parse.quote_plus(str(value))
        query = self.base + query + '&' + self._alter_kwargs['sign'] + '=' + self._get_sign(query)
        try:
            response = requests.get(query).json()
            # time.sleep(random.randint(1, 2))
        except Exception:
            logger.exception('Fail to request.')
            return None

        return response

    # Traverse input df, get api parameters of single call in one row.
    # Send as parameters to call_api(), append response json into result df.
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
                    ref_row = dict()
                    for k, v in row.to_dict().items():
                        ref_row['ref_' + k] = v
                    for call in one_call:
                        call = self.geocode_convert(call)
                        call.update(ref_row)
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

    # Convert lat lon
    @staticmethod
    def geocode_convert(output):
        return output

    # Return md5 encode
    @staticmethod
    def get_md5(raw_sn):
        return hashlib.md5(str(raw_sn).encode('utf-8')).hexdigest()
