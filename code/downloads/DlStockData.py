from DlEastMoney import DownloadData as dle
import pandas as pd
from code.MySql.LoadMysql import StockData1m, LoadBasicInform, LoadNortFunds
from DlDataCombine import download_full_north_funds_to_board

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def stock_1m(code, days):

    try:
        data = dle.stock_1m_multiple(code, days=days)
        date_ = data.iloc[-1]['date'].date()

    except Exception as ex:
        print(f'从东方财富下载{code}异常：{ex};')
        data = pd.DataFrame(data=None)
        date_ = None

    return data, date_  # date_:  data  end date;


def board_1m(code, days):
    try:
        data = dle.board_1m_multiple(code, days=days)
        date_ = data.iloc[-1]['date'].date()

    except Exception as ex:
        print(f'从东方财富下载{code}异常：{ex};')
        data = pd.DataFrame(data=None)
        date_ = None

    return data, date_  # date_:  data  end date;


class DataDailyRenew:  # 近期数据更新

    @classmethod
    def download_1mData(cls):
        shapes = 1

        while shapes:
            record = LoadBasicInform.load_minute()
            industry_records = record[(record['Classification'] == '行业板块') &
                                      (record['EndDate'] < pd.Timestamp('today'))]

            stock_records = record[(record['Classification'] != '行业板块') &
                                   (record['StartDate'] < pd.to_datetime('2020-01-01')) &
                                   (record['EndDate'] < pd.Timestamp('today'))]

            dl = pd.concat([industry_records, stock_records],
                           ignore_index=True).sort_values(by=['EndDate']).reset_index(drop=True)
            print(dl)
            shapes = dl.shape[0]

            if not shapes:
                print('已是最新数据')
                break

            for i in dl.index:
                id_ = dl.loc[i, 'id']
                name = dl.loc[i, 'name']
                EsCode = dl.loc[i, 'EsCode']
                code_ = dl.loc[i, 'code']
                classification = dl.loc[i, 'Classification']

                _ending = dl.loc[i, 'EndDate']
                # ending = None  # 下载数据的日期

                current = pd.Timestamp('today').date()

                days = (current - _ending).days
                # days = days.days
                days = min(5, days)

                print(f'\n下载进度：\n总股票数: {dl.shape[0]}个; 剩余股票: {dl.shape[0] - i}个;')

                if current == _ending:
                    print(f'无最新1m数据:{name}, {code_};')
                    continue

                """ 下载数据"""
                if classification == '行业板块':  # 下载个股1m数据
                    data, ending = board_1m(EsCode, days)

                    if data.shape[0]:
                        data['money'] = 0

                else:
                    data, ending = stock_1m(EsCode, days)

                ''' 判断下载数据是否为空，筛选后数据是否为空'''
                try:
                    select = pd.to_datetime(_ending + pd.Timedelta(days=1))
                    data = data[data['date'] > select]

                    if not data.shape[0]:
                        continue

                except KeyError:
                    continue

                """ 判断是否保存数据 及 更新记录表格 """
                if ending > _ending:

                    try:  # 有时保存数据会出现未知错误
                        # 保存数据， 保存 1m数据;
                        year_ = ending.year
                        StockData1m.append_1m(code_=code_, year_=str(year_), data=data)

                        sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} 
                        set EndDate='{ending}', RecordDate = '{current}', 
                        EsDownload = 'success' where id={id_}; '''
                        LoadBasicInform.basic_execute_sql(sql)

                    except Exception as ex:
                        sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} 
                        set RecordDate = '{current}', EsDownload = 'failed' where id={id_}; '''
                        LoadBasicInform.basic_execute_sql(sql)
                        print(f'股票：{name}, {code_}存储数据异常: {ex}')

                    continue

                if ending == _ending:
                    # 更新数据, record_stock_minute_data;
                    sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} set 
                    EndDate = '{ending}', RecordDate = '{current}' where id = {id_}; '''
                    LoadBasicInform.basic_execute_sql(sql)

                    print(f'{LoadBasicInform.tb_minute} 数据更新成功: {name}, {code_}')
                    continue

    @classmethod
    def renew_NorthFunds(cls):
        tables = ['tostock', 'amount', 'toboard']
        record = LoadBasicInform.load_record_north_funds()
        # print(record)
        # exit()
        for index in record.index:

            table = record.loc[index, 'name']
            id_ = record.loc[index, 'id']

            _ending = record.loc[index, 'ending_date']
            _current = record.loc[index, 'renew_date']

            current = pd.Timestamp('today').date()
            ending = None

            if current <= _current:
                print(f'无新数据:{table}')
                continue

            try:
                if table == tables[0]:  # 北向资金流入个股数据；
                    data = dle.funds_to_stock()

                    if data.shape[0]:
                        ending = data.iloc[-1]['trade_date']
                        LoadNortFunds.append_funds2stock(data)

                if table == tables[1]:
                    data = dle.funds_daily_data()

                    if data.shape[0]:
                        ending = data.iloc[-1]['trade_date']
                        LoadNortFunds.append_amount(data)

                if table == tables[2]:
                    start_ = _ending.strftime('%Y-%m-%d')
                    end_ = current.strftime('%Y-%m-%d')

                    data = download_full_north_funds_to_board(start_, end_)

                    if data.shape[0]:
                        ending = data.iloc[-1]['TRADE_DATE']
                        LoadNortFunds.append_funds2board(data)

            except Exception as ex:
                print(f'{table} 数据更新异常:\n{ex}')

            """ renew record data """
            if not ending or ending <= _ending:
                continue

            sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_record_north_funds} set 
            ending_date = '{ending}', renew_date='{current}' where id={id_}; '''
            LoadBasicInform.basic_execute_sql(sql=sql)

            print(f'{table}数据更新成功;')


class RMDownloadData(DataDailyRenew):

    def __init__(self):
        DataDailyRenew.__init__(self)

    def daily_renew_data(self):
        now = pd.Timestamp('today')
        open1 = pd.to_datetime('09:30')
        close2 = pd.to_datetime('15:30')

        if now < open1:
            self.renew_NorthFunds()  # 北向资金信息

        elif now > close2:
            self.download_1mData()  # 更新股票当天1m信息；
            self.renew_NorthFunds()  # 北向资金信息


if __name__ == '__main__':
    rn = DataDailyRenew()
    rn.renew_NorthFunds()
