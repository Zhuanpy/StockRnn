# -*- coding: utf-8 -*-
import pandas as pd
import time
from DB_MySql import MysqlAlchemy as alc
from DB_MySql import execute_sql

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


class StockData15m:

    db_15m = 'stock_15m_data'

    @classmethod
    def load_15m(cls, code_: str):
        data = alc.pd_read(cls.db_15m, code_)
        return data

    @classmethod
    def append_15m(cls, code_: str, data):
        alc.pd_append(data, cls.db_15m, code_)

    @classmethod
    def replace_15m(cls, code_: str, data):
        alc.pd_replace(data, cls.db_15m, code_)

    @classmethod
    def data15m_execute_sql(cls, sql):
        execute_sql(cls.db_15m, sql)


class StockData1m:
    """  _year: data start year ;  _year: data end year;  """

    @classmethod
    def load_1m(cls, code_: str, _year):

        year_ = int(pd.Timestamp('today').year)

        if len(_year) == 4:
            _year = f'{_year}-01-01'

        _year = int(pd.to_datetime(_year).year)

        data = pd.DataFrame()
        for i in range(year_ - _year + 1):
            db = f'data1m{_year + i}'
            tb = code_.lower()

            data_ = alc.pd_read(db, tb)
            data = pd.concat([data, data_], ignore_index=True)

        return data

    @classmethod
    def append_1m(cls, code_: str, year_: str, data):
        db = f'data1m{year_}'
        tb = code_.lower()
        alc.pd_append(data, db, tb)

    @classmethod
    def replace_1m(cls, code_: str, year_: str, data):
        db = f'data1m{year_}'
        tb = code_.lower()
        alc.pd_replace(data, db, tb)


class StockPoolData:
    db_pool = 'stockpool'

    tb_pool = 'stockpool'
    tb_poolCount = 'poolcount'
    tb_trade_record = 'traderecord'
    tb_board = 'board'

    @classmethod
    def load_StockPool(cls):
        data = alc.pd_read(cls.db_pool, cls.tb_pool)
        return data

    @classmethod
    def load_board(cls):
        data = alc.pd_read(cls.db_pool, cls.tb_board)
        return data

    @classmethod
    def load_poolCount(cls):
        data = alc.pd_read(cls.db_pool, cls.tb_poolCount)
        return data

    @classmethod
    def load_tradeRecord(cls):
        data = alc.pd_read(cls.db_pool, cls.tb_trade_record)
        return data

    @classmethod
    def append_tradeRecord(cls, data):
        alc.pd_append(data, cls.db_pool, cls.tb_trade_record)

    @classmethod
    def append_poolCount(cls, data):
        alc.pd_append(data, cls.db_pool, cls.tb_poolCount)

    @classmethod
    def pool_execute_sql(cls, sql):
        execute_sql(cls.db_pool, sql)


class LoadNortFunds:

    db_funds = 'northfunds'
    tb_amount = 'amount'
    tb_toboard = 'toboard'
    tb_tostock = 'tostock'

    @classmethod
    def load_funds2board(cls):
        data = alc.pd_read(cls.db_funds, cls.tb_toboard)
        return data

    @classmethod
    def append_funds2board(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_toboard)

    @classmethod
    def load_amount(cls):
        data = alc.pd_read(cls.db_funds, cls.tb_amount)
        return data

    @classmethod
    def append_amount(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_amount)

    @classmethod
    def load_funds2stock(cls):
        data = alc.pd_read(cls.db_funds, cls.tb_tostock)
        return data

    @classmethod
    def append_funds2stock(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_tostock)


class LoadRnnModel:

    db_rnn = 'rnn_model'
    tb_train_record = 'trainrecord'
    tb_run_record = 'runrecord'

    @classmethod
    def load_train_record(cls):
        data = alc.pd_read(cls.db_rnn, cls.tb_train_record)
        return data

    @classmethod
    def load_run_record(cls):
        data = alc.pd_read(cls.db_rnn, cls.tb_run_record)
        return data

    @classmethod
    def rnn_execute_sql(cls, sql):
        execute_sql(cls.db_rnn, sql)


class LoadFundsAwkward:

    db_funds_awkward = 'funds_awkward_stock'

    tb_funds_500 = 'topfunds500'
    tb_awkwardNormalization = 'awkward_normalization'
    tb_fundsAwkward = 'fundsawkward'

    @classmethod
    def load_awkwardNormalization(cls):
        data = alc.pd_read(cls.db_funds_awkward, cls.tb_awkwardNormalization)
        return data

    @classmethod
    def append_awkwardNormalization(cls, data):
        alc.pd_append(data, cls.db_funds_awkward, cls.tb_awkwardNormalization)

    @classmethod
    def load_top500(cls):
        data = alc.pd_read(cls.db_funds_awkward, cls.tb_funds_500)
        return data

    @classmethod
    def load_fundsAwkward(cls):
        data = alc.pd_read(cls.db_funds_awkward, cls.tb_fundsAwkward)
        return data

    @classmethod
    def append_fundsAwkward(cls, data):
        alc.pd_append(data, cls.db_funds_awkward, cls.tb_fundsAwkward)

    @classmethod
    def awkward_execute_sql(cls, sql):
        execute_sql(cls.db_funds_awkward, sql)


class LoadBasicInform:

    db_basic = 'stock_basic_information'
    tb_minute = 'record_stock_minute'
    tb_record_north_funds = 'recordnorthfunds'

    @classmethod
    def load_minute(cls):
        data = alc.pd_read(cls.db_basic, cls.tb_minute)
        return data

    @classmethod
    def load_record_north_funds(cls):
        data = alc.pd_read(cls.db_basic, cls.tb_record_north_funds)
        return data

    @classmethod
    def append_record_north_funds(cls, data):
        alc.pd_append(data, cls.db_basic, cls.tb_record_north_funds)

    @classmethod
    def basic_execute_sql(cls, sql):
        execute_sql(cls.db_basic, sql)


if __name__ == '__main__':

    code = '002475'
    year_ = '2021'

    s3 = time.process_time()
    data1 = LoadBasicInform.load_minute()
    data1 = data1[data1['Classification'] == '行业板块']

    print(data1)
    s4 = time.process_time()
    print(f' load_1m time: {s4 - s3}')
