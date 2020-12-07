import random
import time
import re
import pandas as pd
from scrapers import TwoStepScraper
from utils.utility_log import get_logger
from utils.utility_commons import excel_to_df, df_to_excel

logger = get_logger(__name__)


class CN_pc(TwoStepScraper):
    def __init__(self):
        super().__init__()
        self.search_base = 'http://www.yb21.cn'
        self.state = None
        self.city = None
        self.district = None

    # Get items in one page
    def get_item_list(self, pagenum):
        list_soup = self.search(url=self.search_base)
        try:
            item_list = list_soup.find_all('div', attrs={'class': 'citysearch'})
        except Exception:
            logger.exception('Fail to get city list')
            return None

        return item_list

    # Get detail of one item
    def get_item_detail(self, pre_item):
        time.sleep(random.randint(0, 1) / 5.0)
        self.state = pre_item.h1.text.strip()
        items = pre_item.find_all('a')
        # Get item details
        item_detail_list = []

        for item in items:
            self.city = item.text.replace('邮政编码', '').strip()
            item_link = item['href']  # city link
            one_item_soup = self.search(self.search_base + item_link, encoding='GBK')
            try:
                sub_items = one_item_soup.find_all('table')[2].find_all('a')
            except Exception:
                logger.exception('Fail to get district list')
                return None

            for sub_item in sub_items:
                self.district = sub_item.text.strip()
                sub_item_link = sub_item['href']  # district link
                sub_item_soup = self.search(self.search_base + sub_item_link, encoding='GBK')
                try:
                    item_details = sub_item_soup.find_all('tr', attrs={'bgcolor': re.compile('.+')})
                    if len(item_details) < 2:
                        logger.error('Postal code table has no value')
                        return None
                except Exception:
                    logger.exception('Fail to get postal code table')
                    return None
                item_detail_itile = item_details[0].find_all('td')
                for row in item_details[1:]:
                    item_detail = {'state': self.state, 'city': self.city, 'district': self.district}
                    cols = row.find_all('td')
                    item_detail[item_detail_itile[0].text.strip()] = cols[0].text.strip()
                    item_detail[item_detail_itile[1].text.strip()] = cols[1].text.strip()
                    item_detail_list.append(item_detail)
                logger.info('Get {0} {1} {2}'.format(self.state, self.city, self.district))

        return item_detail_list

class CN_pc2(TwoStepScraper):
    def __init__(self):
        super().__init__()
        self.search_base = 'http://alexa.ip138.com'
        self.state = None
        self.city = None
        self.district = None

    # Get items in one page
    def get_item_list(self, pagenum):

        list_soup = self.search(url=self.search_base + '/post', encoding='GBK')
        try:
            item_list = list_soup.find('div', attrs={'id': 'newAlexa'}).find_all('a')
        except Exception:
            logger.exception('Fail to get state list')
            return None

        return item_list

    # Get detail of one item
    def get_item_detail(self, pre_item):
        time.sleep(random.randint(0, 1) / 5.0)

        def _get_district(it, page_suffix=''):
            self.district = it.text.strip()
            if re.match(target_city, it['href']):
                link = self.search_base + self.sp_city + it['href'] + page_suffix
            else:
                link = self.search_base + it['href'] + page_suffix
            one_item_soup = self.search(link, encoding='GBK')
            try:
                sub_items = one_item_soup.find_all('table', attrs={'class': 't6'})[1].find_all('tr')
                sub_items_detail_list = []
                init_sub_item = {'state': self.state, 'city': self.city, 'district': self.district}
                for row in sub_items[1:]:
                    sub_item = init_sub_item.copy()
                    sub_item[sub_items[0].contents[3].text.strip()] = row.contents[1].text.strip()
                    sub_item[sub_items[0].contents[5].text.strip()] = row.contents[2].text.strip()
                    sub_items_detail_list.append(sub_item)
                return sub_items_detail_list
            except Exception:
                logger.exception('Fail to get district list')
                return None
        target_state = re.compile('/post/hainan/|/post/guangdong/|/post/gansu/|/post/henan/')
        target_city = re.compile('sanya/') #('jiayuguan/|zhongshan/|dongwan/|jiyuan/')
        city_pattern = re.compile(r'\w+/')
        if re.match(target_state, pre_item['href']):
            self.sp_city = pre_item['href']
        else:
            return None

        state_url = pre_item['href']

        try:
            items_soup = self.search(self.search_base + state_url, encoding='GBK')
            items = items_soup.find('table', attrs={'class': 't12'}).find_all('a')
        except Exception:
            logger.exception('Fail to get district list')
            return None
        # Get item details
        item_detail_list = []

        for item in items:
            # Get district soup
            if (not re.match(target_city, item['href'])) and re.match(city_pattern, item['href']):
                continue
            else:
                self.district = item.text.strip()
                item_link = item['href']  # district link
                if re.match(target_city, item['href']):
                    cd_link = self.search_base + self.sp_city + item_link
                else:
                    cd_link = self.search_base + item_link
                pre_item_soup = self.search(cd_link, encoding='GBK')
                try:
                    pre_item_info = pre_item_soup.find_all('table', attrs={'class': 't6'})[0].find('th').find_all('a')
                    self.state, self.city = pre_item_info[0].text, pre_item_info[1].text
                    logger.info('Get postal code: {0}, {1}, {2}'.format(self.state, self.city, self.district))
                except Exception:
                    logger.exception('Fail to get state or city')
                    return None

            # Loop page
            page = 1
            while True:
                if page > 1:
                    page_list = _get_district(item, str(page) + '.htm')
                else:
                    page_list = _get_district(item)
                if page_list is not None:
                    item_detail_list += page_list
                    page += 1
                else:
                    break

        return item_detail_list


if __name__ == '__main__':
    # pc = CN_pc2()
    # pc.run(to_page=1)
    # df_group = pc.df.groupby(['state', 'city', 'district', '邮政编码'])['地址'].apply(list).reset_index()
    # df_to_excel(df_group, file_name='pc2')

    import ast
    # pc = excel_to_df(file_name='pc', sheet_name='Results2')
    # pc2 = pc.head(3)
    # pc2['地址'] = pc2['地址'].apply(ast.literal_eval)
    # print(pc2['地址'].loc[0][0])

    pc = excel_to_df(file_name='postal_code', sheet_name='postal codes')
    di = excel_to_df(file_name='postal_code', sheet_name='divisions')
    pc_bk = pc.copy()
    pc['邮政编码'].astype(int)
    pc_group_cd = pc.groupby(['city', 'district'])['邮政编码'].agg(['min', 'max']).reset_index()
    pc_group_cd = pc_group_cd[-(pc_group_cd['city'] == pc_group_cd['district'])]
    pc_group_sc = pc.groupby(['state', 'city'])['邮政编码'].agg(['min', 'max']).reset_index()
    pc_group_s = pc.groupby(['state'])['邮政编码'].agg(['min', 'max']).reset_index()
    pc_group_cd.columns = ['pname', 'fullname', 'pc_min', 'pc_max']
    pc_group_sc.columns = ['pname', 'fullname', 'pc_min', 'pc_max']
    pc_group_s.columns = ['pname', 'pc_min', 'pc_max']

    pc_group_s['fullname'] = pc_group_s['pname']
    pc_group = pd.concat([pc_group_cd, pc_group_sc, pc_group_s])

    pc_agg_cd = pc_bk.groupby(['city', 'district'])['邮政编码'].apply(list).reset_index()
    pc_agg_sc = pc_bk.groupby(['state', 'city'])['邮政编码'].apply(list).reset_index()
    pc_agg_s = pc_bk.groupby(['state'])['邮政编码'].apply(list).reset_index()
    pc_agg_cd.columns = ['pname', 'fullname', 'pc_list']
    pc_agg_sc.columns = ['pname', 'fullname', 'pc_list']
    pc_agg_s.columns = ['pname', 'pc_list']
    pc_agg_s['fullname'] = pc_agg_s['pname']
    pc_agg = pd.concat([pc_agg_cd, pc_agg_sc, pc_agg_s])

    pc_sum = pc_group.merge(pc_agg, on=['pname', 'fullname'], how='left')
    df_to_excel(df=pc_sum, file_name='postal_code', sheet_name='postal_codes_summary')
    fullset = di.merge(pc_sum, left_on=['pname', 'fullname'], right_on=['pname', 'fullname'], how='outer')

    df_to_excel(fullset,file_name='postal_code', sheet_name='division_postal_code')