import pandas as pd
from code.MySql.LoadMysql import LoadRnnModel, StockData1m
from code.Normal import ResampleData
import numpy as np


def reset_record_time(_date):
    time_ = (pd.to_datetime(_date) + pd.Timedelta(days=-150)).date()
    ids = LoadRnnModel.load_run_record()
    ids = tuple(ids['id'])

    sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_run_record} 
    set SignalStartTime = '{time_}', 
    Time15m = '{time_}' where id in {ids};'''

    LoadRnnModel.rnn_execute_sql(sql)


def reset_id_time(id_, _date):
    time_ = (pd.to_datetime(_date) + pd.Timedelta(days=-150)).date()

    sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_run_record} set 
    SignalStartTime = '{time_}', 
    Time15m = '{time_}' where id = {id_};'''

    LoadRnnModel.rnn_execute_sql(sql)


def date_range(_date, date_, code_='bk0424') -> list:

    _date = pd.to_datetime(_date)
    date_ = pd.to_datetime(date_)

    data = StockData1m.load_1m(code_, _year=str(_date.year))
    data = ResampleData.resample_1m_data(data=data, freq='day').drop_duplicates(subset=['date'])
    data = data[(data['date'] >= _date) & (data['date'] <= date_)]
    data = list(data['date'])

    return data



