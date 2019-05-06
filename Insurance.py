#coding=utf-8
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re



page = 1


df = pd.DataFrame(columns=['title', 'name', 'origin', 'target', 'date'])
while page <= 15:
    query = 'http://bxjg.circ.gov.cn/dig/search.action?siteId=&ty=&w=false&f=&dr=true&p=' + str(
        page) + '&sr=score+desc&rp=&advtime=&advrange=title&fq=tabName%3A%E8%A1%8C%E6%94%BF%E8%AE%B8%E5%8F%AF&ext=siteid%3A33&firstq=%E5%8F%98%E6%9B%B4%E8%90%A5%E4%B8%9A%E5%9C%BA&q=%E5%8F%98%E6%9B%B4%E8%90%A5%E4%B8%9A%E5%9C%BA'
    query2 = 'http://bxjg.circ.gov.cn/dig/search.action?siteId=33&ty=&w=false&f=&dr=true&p=' + str(
        page) + '&sr=score+desc&rp=&advtime=0&advrange=title&fq=tabName%3A%E7%BB%93%E6%9E%9C%E5%85%AC%E5%B8%83&ext=siteid%3A33&firstq=%E5%8F%98%E6%9B%B4%E8%90%A5%E4%B8%9A%E5%9C%BA&q=%E5%8F%98%E6%9B%B4%E8%90%A5%E4%B8%9A%E5%9C%BA'
    response = requests.get(query2)
    soup_response = BeautifulSoup(response.text, 'lxml')
    urls = soup_response.find_all('h3')
    queryp = re.compile(r'.*变更营业场.*')

    for url in urls:
        query_detail = url.find('a')

        if query_detail is None:
            continue
        elif not re.findall(queryp, query_detail.text):
            continue
        else:
            try:
                query_detail = query_detail.get('href')
                response_detail = requests.get(query_detail)
                print(query_detail)
                response_detail.content.decode('utf-8')
                soup_detail = BeautifulSoup(response_detail.content, 'html.parser', from_encoding="utf8")

                title = str(soup_detail.title.text).strip()
                name = None
                origin = None
                target = None
                dateg = None

                for p in soup_detail.find_all('p'):
                    spacep = re.compile(r'\s')
                    text = re.sub(spacep, '', p.text)
                    namep = re.compile(r'.*公司：')
                    namese = re.search(namep, text)
                    addressp = re.compile(r'“.*”*')
                    address = re.findall(addressp, text)
                    datep = re.compile(r'\d*年\d*月\d*日')
                    datelist = re.findall(datep, text)

                    if namese is not None:
                        namestart = namese.span()[0]
                        nameend = namese.span()[1]
                        name = str(p.text[namestart:nameend-1]).strip()
                    elif address:

                        address = address[0].replace('“', '').replace('”', '').replace('。', '').split('变更为')

                        origin = str(address[0]).strip()
                        target = str(address[1]).strip()
                    elif datelist:

                        dateg = datelist[0]

            except:
                continue

            row = {'title': title, 'name': name, 'origin': origin, 'target': target, 'date': dateg}
            df = df.append(row, ignore_index=True)
    page = page + 1
    print(page)


df.to_excel(r'C:\Users\Benson.Chen\Desktop\Scraper\Result\SZ_Insurance2.xlsx', index=False, header=True, columns=list(df), sheet_name='结果公布')


