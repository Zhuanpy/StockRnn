# -*- coding: utf-8 -*-
from DlJuQuan import DownloadData as dlj
from DlEastMoney import DownloadData as dle
from code.MySql.LoadMysql import LoadFundsAwkward, LoadBasicInform, StockData1m
from code.RnnModel.Rnn_utils import date_range
from code.MySql.DB_MySql import MysqlAlchemy as ml
import pandas as pd
import pandas
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def download_1m(stock, code, days):

    try:
        data_1m = dle.stock_1m_multiple(code, days=days)

    except Exception as ex:
        print(f'东方财富下载{stock}1m数据异常：{ex};')
        data_1m = pd.DataFrame()

    return data_1m


def collect_full_data1m():  # 补充 完整的 1m_data 数据库;

    awkward = LoadFundsAwkward.load_fundsAwkward()
    awkward = awkward.groupby('stock_name').count().sort_values(by=['funds_name']).reset_index().tail(300)
    awkward = list(awkward['stock_name'])

    # 确定哪些股票需下载；
    basic = LoadBasicInform.load_minute()
    basic = basic[(basic['name'].isin(awkward)) &
                  (basic['StartDate'] > pd.to_datetime('2018-06-01')) &
                  (basic['EndDate'] < pd.Timestamp('today')) &
                  (~basic['Classification'].isin(['科创板', '创业板']))].reset_index(drop=True)

    print(basic)
    over_ = ''
    over = '您的1000万条体验期已结束'

    if basic.shape[0]:
        years = [2018 + i for i in range(pd.Timestamp('today').year - 2018 + 1)]
        years = sorted(years, reverse=True)  # [2022, 2021, 2020, 2019, 2018]

        for year_ in years:

            for index in basic.index:
                name = basic.loc[index, 'name']
                code = basic.loc[index, 'code']
                id_ = basic.loc[index, 'id']

                record_start = basic.loc[index, 'StartDate']
                record_year = pd.to_datetime(record_start).year

                if year_ <= record_year:  # example: record_year: 2021 , year_: 2022 or 2021 or 2020

                    end_ = (pd.to_datetime(record_start) + pd.Timedelta(days=-1)).date()
                    start_ = pd.to_datetime(f'{year_}-01-01').date()

                    print(f'下载： {name}, {code},时间段 {start_} 至 {end_}；')

                    if start_ <= end_:
                        try:
                            data1m = dlj.download_history_data(code, frequency='1m', fq_value='不复权',
                                                               start_date=str(start_), end_date=str(end_))
                            if data1m.shape[0]:
                                start_new = data1m.iloc[0]['date'].date()

                                try:  # 保存数据
                                    _data = StockData1m.load_1m(code, _year=year_)
                                    data1m = pd.concat([data1m, _data], ignore_index=True)

                                    data1m = data1m.drop_duplicates(subset=['date']). \
                                        sort_values(by=['date']).reset_index(drop=True)

                                    StockData1m.replace_1m(code_=code, year_=str(year_), data=data1m)

                                except pandas.io.sql.DatabaseError:
                                    StockData1m.replace_1m(code_=code, year_=str(year_), data=data1m)

                                except Exception as ex:
                                    over_ = str(ex).split('，')[0]

                                # 更新参数
                                sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} set 
                                StartDate = '{start_new}' where id = {id_};'''
                                LoadBasicInform.basic_execute_sql(sql=sql)

                                print(f'下载成功: {name}, {code} 1m 数据;')
                                time.sleep(10)

                        except Exception as ex:

                            print(f'下载 {name}, {code} 1m数据异常;\n{ex}')

                            if str(ex) == 'Cannot convert non-finite values (NA or inf) to integer':  # 数据下载错误时

                                sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} set 
                                StartDate = '1990-01-01', 
                                EndDate = '2050-01-01', 
                                RecordDate = '2050-01-01' where id = {id_};'''

                                LoadBasicInform.basic_execute_sql(sql=sql)

                            over_ = str(ex).split('，')[0]

                    if over_ == over:
                        break

    else:
        print('无历史分时数据需下载;')


def download_full_north_funds_to_board(start_: str, end_: str):
    """
    _date:  pre date data;
    date_: end date data or planed download date data;
    """
    df = pd.DataFrame()

    t = 5

    while t:

        _date = ml.pd_read(database='northfunds', table='toboard')
        _date = _date[_date['TRADE_DATE'] >= pd.to_datetime(start_)]
        _date = _date.drop_duplicates(['TRADE_DATE'])
        _date = list(_date['TRADE_DATE'])

        date_ = date_range(start_, end_)

        date_ = [i for i in date_ if i not in _date]

        print(date_)

        if not len(date_):
            t = 0

        if len(date_):
            t = t - 1

            for d in date_:
                d = d.strftime('%Y-%m-%d')
                dl = dle.funds_to_sectors(d)

                if not dl.shape[0]:
                    time.sleep(5)
                    continue

                df = pd.concat([df, dl])

                ml.pd_append(dl, 'northfunds', 'toboard')

                time.sleep(5)

    return df


if __name__ == '__main__':
    s = '2022-05-01'
    e = '2022-08-08'
    d = download_full_north_funds_to_board(s, e)

    print(d)
