import time
from code.MySql.LoadMysql import StockPoolData


def get_times():
    str_time = time.strftime('%Y{}%m{}%d{} %X')
    str_time = str_time.format('年', '月', '日')
    return str_time


def get_c1_data():

    data = StockPoolData.load_poolCount()

    tp = (data.loc[0, 'Up'],
          data.loc[1, 'Up'],
          data.loc[3, 'Up'],
          data.loc[4, 'Up'])
    return tp


if __name__ == '__main__':
    print(get_c1_data())
