import pandas as pd
from code.TrendDistinguish.TrendDistinguishRunModel import TrendDistinguishModel
from code.MySql.sql_utils import Stocks
from code.MySql.LoadMysql import StockPoolData, LoadBasicInform
import multiprocessing


def distinguish_board(code_, date_, id_=None, freq='120m'):

    if not date_:
        date_ = pd.Timestamp('today').date()

    else:
        date_ = pd.to_datetime(date_).date()

    if not id_:
        name, code_, id_ = Stocks(code_)

    dis = TrendDistinguishModel()
    label, value_ = dis.distinguish_1m(code_=code_, freq=freq, date_=date_)

    ''' 更新数据 '''
    sql = f'''update {StockPoolData.db_pool}.{StockPoolData.tb_board} 
    set Trends= '{value_}', 
    RecordDate='{date_}' where id='{id_}';'''

    StockPoolData.pool_execute_sql(sql)


def board_evaluate(day_, _num, num_, data):

    count = 0

    for index in range(_num, num_):
        stock_ = data.loc[index, 'code']
        id_ = data.loc[index, 'id']

        print(f'当前进度，剩余{num_ - _num - count}；')
        try:
            distinguish_board(code_=stock_, id_=id_, date_=day_)

        except Exception as ex:
            print(f'Error: {ex}')

        count += 1


def multiprocessing_count_board(date_):

    data = LoadBasicInform.load_minute()
    data = data[data['Classification'] == '行业板块']

    shape_ = data.shape[0]

    print(f'处理板块日期: {date_}, 处理个数：{shape_}')

    l1 = shape_ // 3
    l2 = shape_ // 3 * 2

    p1 = multiprocessing.Process(target=board_evaluate, args=(date_, 0, l1, data))
    p2 = multiprocessing.Process(target=board_evaluate, args=(date_, l1, l2, data))
    p3 = multiprocessing.Process(target=board_evaluate, args=(date_, l2, shape_, data))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()
