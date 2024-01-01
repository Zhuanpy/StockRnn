# -*- coding: utf-8 -*-
import time
import json
import re
from selenium import webdriver
from bs4 import BeautifulSoup as soup
import pandas as pd
from download_utils import page_source, WebDriver
from root_ import file_root
from download_utils import UrlCode


def data_headers(pp: str):
    _path = file_root()
    pph = f'{_path}/pp/EastMoney/header_{pp}.txt'
    f2 = open(pph, 'r')
    lines = f2.readlines()
    headers = {}

    for line in lines:
        line = line.strip('\n')
        line = line.replace(' ', '')
        line = line.split(':')
        keys = line[0]
        values = line[1]
        headers[keys] = values

    return headers


def data_url(pp: str):
    _path = file_root()
    ppl = f'{_path}/pp/EastMoney/Url_{pp}.txt'
    f2 = open(ppl, 'r')
    lines = f2.readlines()
    url = lines[0]
    url = url.strip('\n')
    url = url.replace(' ', '')
    return url


def print_dl(freq, code):
    print(f'Success Download {freq} Data: {code};')


def return_FundsData(source, date_new):
    columns = ['序号', 'stock_code', 'stock_name', '相关', '今日收盘价', '今日涨跌幅', '今日持股股数',
               '今日持股市值', '今日持股占流通股比', '今日持股占总股本比', '今日增持股数',
               '今日增持市值', '今日增持市值增幅', '今日增持占流通股比', '今日增持占总股本比', 'industry']

    df = pd.read_html(source)[1]
    df.columns = columns
    df.loc[:, 'trade_date'] = pd.to_datetime(date_new)
    df = df[['trade_date', 'stock_code', 'stock_name', 'industry']]
    df['stock_code'] = df['stock_code'].astype(str)

    return df


def conversion(x):  # 货币单位转换函数  Unit conversion

    if x == '亿':
        values = 100000000

    elif x == '万':
        values = 10000

    elif x == '百万':
        values = 1000000

    elif x == '千万':
        values = 10000000

    else:
        values = 1

    return values


def FundsDataClean(data):
    data = pd.DataFrame(data.values)
    data = data[[1, 5, 6, 7, 9, 12]]
    data = data.rename(columns={1: '板块', 5: 'NkPT市值', 6: 'NkPT占板块比',
                                7: 'NkPT占北向资金比', 9: 'NRPT市值', 12: 'NRPT占北向资金比'})

    data.loc[:, 'Unit_NKPT'] = data['NkPT市值'].str[-1:]
    data.loc[:, 'NkPT市值'] = data['NkPT市值'].str[:-1].astype(float) * data.Unit_NKPT.apply(lambda x: conversion(x))
    data.loc[:, 'Unit_NRPT'] = data['NRPT市值'].str[-1]
    data.loc[:, 'NRPT市值'] = data['NRPT市值'].str[:-1].astype(float) * data.Unit_NRPT.apply(lambda x: conversion(x))

    data = data.drop(columns=['Unit_NKPT', 'Unit_NRPT'])

    return data


def get_1m_data(source, match=False, multiple=False):
    # 最小匹配，保留括号内的Json 数据
    if match:
        p1 = re.compile(r'[(](.*?)[)]', re.S)
        source = re.findall(p1, source)
        source = json.loads(source[0])['data']['trends']

    else:
        source = json.loads(source)['data']['trends']

    source = [x.split(',') for x in source]

    # 构建空的Dataframe 保留数据
    columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']
    df = pd.DataFrame(source, columns=columns)

    # 将日期数据类型更改为 datetime
    df['date'] = pd.to_datetime(df['date'])

    # 将数据类型更改为 float
    flt = ['open', 'close', 'high', 'low', 'volume', 'money']
    df[flt] = df[flt].astype(float)

    # 将数据类型更改为 整数
    df['volume'] = df['volume'] * 100
    df[['volume', 'money']] = df[['volume', 'money']].astype('int64')

    # 清理 09：30 时间数据，合并成09:31分数据
    if multiple:
        df['day'] = df['date'].dt.date
        news = pd.DataFrame()

        for index in df.drop_duplicates(subset=['day']).index:

            # 多个日期，按照日期来整理  09:30 & 09:31 数据
            d1 = df.loc[index, 'day']
            df2 = df[df['day'] == d1].reset_index(drop=True)
            df2.loc[1, 'volume'] = df2.loc[0, 'volume'] + df2.loc[1, 'volume']
            df2.loc[1, 'money'] = df2.loc[0, 'money'] + df2.loc[1, 'money']
            df2 = df2.iloc[1:].reset_index(drop=True)

            news = pd.concat([news, df2], ignore_index=True)

        news = news[columns]

    else:
        df.loc[1, 'volume'] = df.loc[0, 'volume'] + df.loc[1, 'volume']
        df.loc[1, 'money'] = df.loc[0, 'money'] + df.loc[1, 'money']

        news = df.iloc[1:].reset_index(drop=True)

    return news


class DownloadData:  # 从东方财富下载数据

    @classmethod
    def stock_1m_data(cls, code: str):  # 从东方财富网下载1分钟股票数据
        headers = data_headers('stock_1m_data')
        codes = UrlCode(code)
        url = data_url('stock_1m_data').format(codes)

        source = page_source(url=url, headers=headers)
        dl_ = get_1m_data(source=source, match=True, multiple=False)

        print_dl('1m', code)
        return dl_

    @classmethod
    def stock_1m_multiple(cls, code: str, days=5):  # 从东方财富网下载1分钟股票数据
        headers = data_headers('stock_1m_multiple_days')
        codes = UrlCode(code)
        url = data_url('stock_1m_multiple_days')
        url = url.format(days, codes)  # 东方财富分时数据网址

        source = page_source(url=url, headers=headers)

        dl = get_1m_data(source=source, match=True, multiple=True)  # 处理下载数据

        print_dl('1m', code)  # 打印下载
        return dl

    @classmethod
    def board_1m_data(cls, code: str):
        headers = data_headers('board_1m_data')
        url = data_url('board_1m_data').format(code)
        source = page_source(url=url, headers=headers)
        dl = get_1m_data(source, match=True, multiple=False)
        print_dl('1m', code)  # 打印下载
        return dl

    @classmethod
    def board_1m_multiple(cls, code: str, days=5):
        headers = data_headers('board_1m_multiple_days')
        url = data_url('board_1m_multiple_days').format(code, days)
        source = page_source(url=url, headers=headers)
        dl = get_1m_data(source, match=False, multiple=True)
        print_dl('1m', code)
        return dl

    @classmethod
    def funds_to_stock(cls):  # 从东方财富网下载北向资金流入个股数据：

        # 下载北向资金每日流向数据，通过北向资金流入，选择自己的股票池; 北向资金已经下载数据； 北向资金个股流入个股网址
        page1 = 'http://data.eastmoney.com/hsgtcg/list.html'
        page2 = '/html/body/div[1]/div[8]/div[2]/div[2]/div[2]/div[3]/div[3]/div[1]/a[2]'
        page3 = '/html/body/div[1]/div[8]/div[2]/div[2]/div[2]/div[3]/div[3]/div[1]/a[4]'
        path_date = '/html/body/div[1]/div[8]/div[2]/div[2]/div[1]/div[1]/div/span'
        driver = webdriver.Chrome()
        driver.get(page1)
        new_date = pd.to_datetime(driver.find_element_by_xpath(path_date).text[1:-1])

        # 获取第1页50条数据
        source01 = driver.page_source
        dl01 = return_FundsData(source01, new_date)

        # 获取第2页50条数据
        driver.find_element_by_xpath(page2).click()
        time.sleep(6)
        source02 = driver.page_source
        dl02 = return_FundsData(source02, new_date)

        # 获取第3页50条数据
        driver.find_element_by_xpath(page3).click()
        time.sleep(6)
        source03 = driver.page_source
        dl03 = return_FundsData(source03, new_date)
        driver.close()

        # 合并数据：
        data = pd.concat([dl01, dl02, dl03], ignore_index=True).reset_index(drop=True)

        print(f'东方财富下载{new_date}日北向资金流入个股据成功;')
        return data

    @classmethod
    def funds_month_history(cls):  # 北向资金近1个月流入
        headers = data_headers('funds_month_history')
        url = data_url('funds_month_history')

        PageSource = page_source(url=url, headers=headers)

        p1 = re.compile(r'[(](.*?)[)]', re.S)  # 最小匹配
        dl = re.findall(p1, PageSource)[0]
        dl = pd.DataFrame(data=json.loads(dl)['result']['data'])
        dl = dl[['TRADE_DATE', 'NET_INFLOW_SH', 'NET_INFLOW_SZ', 'NET_INFLOW_BOTH']]
        dl.loc[:, 'TRADE_DATE'] = pd.to_datetime(dl['TRADE_DATE']).dt.date
        dl = dl.rename(columns={'TRADE_DATE': 'trade_date'})
        print('东方财富下载近一个月北向资金数据成功;')
        return dl

    @classmethod
    def funds_daily_data(cls):
        web_01 = 'https://data.eastmoney.com/hsgt/'
        driver = webdriver.Chrome()
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)
        driver.get(web_01)

        # 更新时间 07-30
        update_xpath = '/html/body/div[1]/div[8]/div[2]/div[2]/div[3]/div[1]/div[3]/span'

        # 沪股通 净流入
        SH_money_xpath = '/html/body/div[1]/div[8]/div[2]/div[2]/div[3]/div[6]/ul[1]/li[1]/span[2]/span/span'

        # 深股通 净流入
        SZ_money_xpath = '/html/body/div[1]/div[8]/div[2]/div[2]/div[3]/div[6]/ul[1]/li[2]/span[2]/span/span'

        # 北向 净流入
        North_sum_xpath = '/html/body/div[1]/div[8]/div[2]/div[2]/div[3]/div[6]/ul[1]/li[3]/span[2]/span/span'

        update = driver.find_element_by_xpath(update_xpath).text
        SH_money = float(driver.find_element_by_xpath(SH_money_xpath).text[:-2]) * 100
        SZ_money = float(driver.find_element_by_xpath(SZ_money_xpath).text[:-2]) * 100
        North_sum = float(driver.find_element_by_xpath(North_sum_xpath).text[:-2]) * 100

        data_dic = {'trade_date': [pd.to_datetime(update)], 'NET_INFLOW_SH': [SH_money],
                    'NET_INFLOW_SZ': [SZ_money], 'NET_INFLOW_BOTH': [North_sum]}

        data = pd.DataFrame(data=data_dic)
        driver.close()
        print(f'东方财富下载{update}日北向资金成功;')
        return data

    @classmethod
    def funds_to_sectors(cls, date_: str):
        headers = data_headers('funds_to_sectors')
        url = data_url('funds_to_sectors').format(date_)
        PageSource = page_source(url=url, headers=headers)

        try:
            p1 = re.compile(r'[(](.*?)[)]', re.S)
            page_data = re.findall(p1, PageSource)
            json_data = json.loads(page_data[0])
            json_data = json_data['result']['data']

            value_list = []
            for i in range(len(json_data)):
                values = list(json_data[i].values())
                value_list.append(values)

            key_list = list(json_data[0])

            download = pd.DataFrame(data=value_list, columns=key_list)

            drops = ['BOARD_INNER_CODE', 'BOARD_TYPE', 'MINADD_RATIO_SECUCODE',
                     'IS_NEW', 'INTERVAL_TYPE', 'COMPOSITION_QUANTITY_ADD',
                     'MAXADD_RATIO_SECUCODE', 'MINADD_SECUCODE',
                     'ORIG_BOARD_CODE', 'MAXADD_SECUCODE', 'MAXHOLD_MARKETCAP_NAME',
                     'MAXHOLD_MARKETCAP_SECURITYCODE', 'MAXHOLD_MARKETCAP_SECUCODE']

            download = download.drop(columns=drops)

            download.loc[:, 'TRADE_DATE'] = pd.to_datetime(download['TRADE_DATE']).dt.date

        except TypeError:
            print(f'东方财富下载 Funds to Sectors 数据异常;')
            download = pd.DataFrame(data=None)

        return download

    @classmethod
    def industry_list(cls):  # 下载板块组成
        web = 'http://quote.eastmoney.com/center/boardlist.html#industry_board'
        driver = webdriver.Chrome()
        driver.get(web)

        source = driver.page_source
        bs_data = soup(source, 'html.parser')
        board_data = bs_data.find('li', class_='sub-items menu-industry_board-wrapper')
        board_data = board_data.find_all('li')
        data = pd.DataFrame(data=None)

        for i in range(len(board_data)):
            board_name = board_data[i].find(class_='text').text
            board_code = str(board_data[i].find('a')['href']).strip()[-6:]
            data.loc[i, 'board_name'] = board_name
            data.loc[i, 'board_code'] = board_code

        driver.close()
        data.loc[:, 'stock_name'] = None
        data.loc[:, 'stock_code'] = None
        return data

    @classmethod
    def industry_ind_stock(cls, name, code, num=300):  # 下载板块成份股
        url = data_url('industry_ind_stock').format(num, code)
        headers = data_headers('industry_ind_stock')
        source = page_source(url=url, headers=headers)

        dl = None
        if source:
            p1 = re.compile(r'[(](.*?)[)]', re.S)
            page_data = re.findall(p1, source)
            json_data = json.loads(page_data[0])['data']['diff']

            values_list = []
            for i in range(len(json_data)):
                values = list(json_data[i].values())
                values_list.append(values)

            key_list = list(json_data[0])
            dl = pd.DataFrame(data=values_list, columns=key_list)
            dl = dl.rename(columns={
                'f3': '涨跌幅', 'f4': '涨跌额', 'f5': '成交量', 'f6': '成交额', 'f7': '振幅', 'f8': '换手率',
                'f9': '市盈率动', 'f10': '量比', 'f12': 'stock_code', 'f14': 'stock_name', 'f15': 'close',
                'f16': 'low', 'f17': 'open', 'f18': 'preclose', 'f20': '总市值', 'f21': '流通市值', 'f23': '市净率',
                'f115': '市盈率'})

            dl = dl.drop(columns=['f1', 'f2', 'f11', 'f13', 'f22', 'f24', 'f25', 'f45',
                                  'f62', 'f128', 'f140', 'f141', 'f136', 'f152'])
            dl.loc[:, 'board_name'] = name
            dl.loc[:, 'board_code'] = code
            dl.loc[:, 'date'] = pd.Timestamp('today').date()

            dl = dl[['board_name', 'board_code', 'stock_code', 'stock_name', 'date']]

        return dl

    @classmethod
    def funds_awkward(cls, code):
        url = data_url('funds_awkward').format(code)
        headers = data_headers('funds_awkward')
        source = page_source(url=url, headers=headers)
        source = soup(source, 'lxml')
        source = source.find("tbody")
        stocks = source.find_all("tr")
        li_name = []
        li_code = []
        for i in stocks:
            name = i.find(class_="tol").text.replace(' ', '')
            code = i.find_all('td')[1].text.replace(' ', '')
            li_name.append(name)
            li_code.append(code)
        dic = {'stock_name': li_name, 'stock_code': li_code}
        data = pd.DataFrame(dic)
        return data

    @classmethod
    def funds_awkward_by_driver(cls, code):

        try:
            url = f'http://fund.eastmoney.com/{code}.html'
            driver = WebDriver()
            driver.get(url)
            source = driver.page_source
            source = soup(source, 'html.parser')
            source = source.find_all('li', class_='position_shares')[0].find_all('td', class_='alignLeft')
            driver.close()

            li = []
            for i in source:
                name = i.text.replace(' ', '')
                li.append(name)

            dic = {'stock_name': li}
            data = pd.DataFrame(dic)

        except ValueError:
            data = pd.DataFrame()

        return data


if __name__ == '__main__':
    # data = DownloadData.stock_1m_multiple(code='002475')
    dl = DownloadData()
    data = dl.stock_1m_multiple('002475')
    print(data)
