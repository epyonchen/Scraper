import pandas as pd
import keys
import requests
import time
import random
import gecodeconvert as gc


def search_location_api_call(keyword_query, key, **kwargs):
    query = 'http://api.map.baidu.com/place/v2/search?query=' + keyword_query
    for arg in kwargs.keys():
        query = query + '&' + arg + '=' + str(kwargs[arg])
    query = query + '&ak=' + key
    print(query)
    response = requests.get(query)
    response = response.json()
    time.sleep(random.randint(1, 2))
    return response


def search_location(keyword_inputs, city_inputs, location_input):
    df = pd.DataFrame(columns=['uid', 'keyword', 'name', 'province', 'city', 'area', 'address', 'lat', 'lon', 'tag', 'type', 'overall_rating', 'comment_num', 'MapITlat', 'MapITlon'])

    for city in city_inputs:
        for keyword in keyword_inputs:
            page = 1
            print('df size:', len(df))
            while page > 0:
                response = search_location_api_call(keyword, keys.baidu, tag='美食' ,region=city, city_limit=0, scope=2, page_size=20, page_num=(page-1), output='json')
                # response = search_location_api_call(keyword, keys.baidu, location='23.123892,113.332952', radius_limit='false', scope=2, radius=500, page_size=20, page_num=(page-1), output='json')
                if 'total' not in response.keys() or response['total'] < 1 or response['results'] == []:
                    page = 0
                    break
                else:
                    try:
                        page += 1
                        for store in response['results']:
                            store['keyword'] = keyword
                            store['lat'] = store['location']['lat']
                            store['lon'] = store['location']['lng']
                            mapit = geocode_convert(store['location']['lat'], store['location']['lng'])
                            store['MapITlat'] = mapit[1]
                            store['MapITlon'] = mapit[0]
                            if 'detail_info' in store.keys():
                                if 'overall_rating' in store['detail_info'].keys():
                                    store['overall_rating'] = store['detail_info']['overall_rating']
                                else:
                                    store['overall_rating'] = None
                                if 'comment_num' in store['detail_info'].keys():
                                    store['comment_num'] = store['detail_info']['comment_num']
                                else:
                                    store['comment_num'] = None
                                if 'tag' in store['detail_info'].keys():
                                    store['tag'] = store['detail_info']['tag']
                                else:
                                    store['tag'] = None
                                if 'type' in store['detail_info'].keys():
                                    store['type'] = store['detail_info']['type']
                                else:
                                    store['type'] = None
                            # else:
                            #     store['overall_rating'] = None
                            #     store['comment_num'] = None
                            #     store['tag'] = None
                            #     store['type'] = None

                            inflag = validate_in(store, location_input)
                            if inflag:
                                df = df.append(store, ignore_index=True)
                            else:
                                continue
                    except:
                        print('No match.')
                        break

    return df


# Convert from Baidu to
def geocode_convert(lat, lon):

    return gc.bd09_to_wgs84(lon, lat)


def validate_in(store, location_in):
    for loc in location_in:
        if (loc in store['address']) or (loc in store['name']):
            return True

    return False


if __name__ == '__main__':
    # city_list = ['广州', '深圳', '佛山', '珠海', '东莞', '惠州', '肇庆', '中山']
    location_in = ['k11', '周大福', '珠江东路6号']
    keyword_list = ['k11']
    # keyword_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Archive\Project list for TDIM.xlsx', sheet_name='Sheet1')
    # keyword_list = keyword_list['keyword'].tolist()
    city_list = ['广州']
    location_list = search_location(keyword_list, city_list, location_in)
    location_list.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\CAA_k11_search3.xlsx', index=False, header=True, columns=list(location_list), sheet_name='Baidu Api')
