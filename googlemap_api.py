# -*- coding: utf-8 -*-
"""
Created on Sun March 20th 2019

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import keys
import urllib
import requests
import time
import random
import datetime
import gecodeconvert as gc


class GoogleMap:

    def __init__(self, function='place/findplacefromtext/', output='json?'):

        self.parameter = {'language': 'en', 'key': keys.google}
        if function == 'place/findplacefromtext/':
            findplace_parameter = {'input': '', 'inputtype': 'textquery'}
            self.parameter.update(findplace_parameter)
        self.base = 'https://maps.googleapis.com/maps/api/'
        self.function = function
        self.output = output
        self.searchbase = ''
        self.query = ''
        self.init_searchbase()

    def init_searchbase(self):
        self.searchbase = self.base + self.function + self.output

    # Initial query with paramter
    def init_query(self):
        self.query = self.searchbase + urllib.parse.urlencode(self.parameter)

    # Do an API search
    def search(self, **kwargs):
        if kwargs is not None:
            self.parameter.update(kwargs)
        self.init_query()
        try:
            response = requests.get(self.query).json()
            if ('status' not in response.keys()) or (response['status'] != 'OK'):
                print('Wrong query1')
                print(response)
                return None
            else:
                return self.convert_response(response)
        except:
            print('Wrong query2')
            print(response)
            return False

    # Format response
    def convert_response(self, response):
        if self.function == 'place/findplacefromtext/':
            response = response['candidates'][0]

        elif self.function == 'place/details/':
            response = response['result']
            if 'address_components' in response.keys():
                for addr_info in response['address_components']:
                    response[addr_info['types'][0]] = addr_info['long_name']
        else:
            return None

        # get lat&lon, convert into wgs84
        response['lat'] = response['geometry']['location']['lat']
        response['lon'] = response['geometry']['location']['lng']
        mapit = self.geocode_convert(response['lat'], response['lon'])
        response['MapITlat'] = mapit[1]
        response['MapITlon'] = mapit[0]

        return response

    def geocode_convert(self, lat, lon):
        return gc.gcj02_to_wgs84(lon, lat)

    # def get_parameter(self):
    #     parameter = ''
    #     for key, value in self.parameter:
    #         parameter = parameter + key + '=' + value
    #     return parameter

    # def update_parameter(self, **kwargs):
    #     temp = kwargs
    #     temp.update(self.parameter)
    #     self.parameter = temp


if __name__ == '__main__':
    site = 'Property_GoogleMap_ENnameaddress'
    folder = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Address Doctor'
    date = str(datetime.date.today())
    path = folder + '\{}_{}.xlsx'.format(site, date)
    writer = pd.ExcelWriter(path, engine='openpyxl')

    input_list = pd.read_excel(folder + '\Address Doctor Test Case_20190308.xlsx', sheet_name='EN_CN', sort=False)
    input_list = input_list.fillna('').query('Pick in (1,2,5)')

    place_search = GoogleMap()
    place_detail = GoogleMap(function='place/details/')

    place_search_df = pd.DataFrame()
    place_detail_df = pd.DataFrame()

    count = 0
    for index, property in input_list.iterrows():
        print(property['Property_Name'])

        # if property['Case'] in [1, 2]:
        #     input = property['Property_Name'] + ' ' + property['City']
        #     place_search_response = place_search.search(input=input, fields='id,place_id,name,formatted_address,geometry/location')
        # elif property['Case'] == 5:
        #     input = property['District'] + property['Address_1'] + ' ' + property['City']
        #     place_search_response = place_search.search(input=input, fields='id,place_id,name,formatted_address,geometry/location')

        inputtext = property['Property_Name'] + ' ' + property['Address_1'] + ' ' + property['City']
        place_search_response = place_search.search(input=inputtext, fields='id,place_id,name,formatted_address,geometry/location')
        if place_search_response is None:
            place_search_response = dict()

        place_search_response['Property_Code'] = property['Property_Code']
        place_search_df = place_search_df.append(place_search_response, ignore_index=True)
        if 'place_id' in place_search_response.keys():
            place_detail_response = place_detail.search(placeid=place_search_response['place_id'], fields='id,place_id,name,address_component,formatted_address,geometry/location')
            if place_detail_response is None:
                place_detail_response = dict()
            place_detail_response['Property_Code'] = property['Property_Code']
            place_detail_df = place_detail_df.append(place_detail_response, ignore_index=True)

        # count += 1
        # if count > 100:
        #     break

    place_search_df.to_excel(writer, index=False, header=True, sheet_name='Place Search')
    place_detail_df.to_excel(writer, index=False, header=True, sheet_name='Place Detail')
    writer.save()
    writer.close()
