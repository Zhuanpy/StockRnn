import sys
from code.MySql.LoadMysql import StockPoolData
from pywinauto import Application
from pywinauto.keyboard import send_keys
import pyautogui as ag
from time import sleep

import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


class TongHuaShunAutoTrade:

    def __init__(self):
        self.app, self.win = self.start_app()

    def start_app(self):
        app_ = Application(backend='uia').start('E:/Soft_for_trading/Trading_TonghuaShun/xiadan.exe')  # backend='uia'
        win_ = app_.window(class_name="网上股票交易系统5.0")
        sleep(3)
        return app_, win_

    def sleep2stop(self, s=0.1):
        x, y = ag.position()
        if x > 1200 or y > 665:
            sys.exit()
        else:
            sleep(s)

    def actions(self, code_, num_, price_, buy_=True):

        # 证券代码: (x=307, y=121)，
        ag.moveTo(x=307, y=121)
        self.sleep2stop()

        ag.doubleClick()
        self.sleep2stop()

        send_keys(code_)
        self.sleep2stop(1)

        # 买入价格: Point(x=304, y=164)，
        if price_:
            ag.moveTo(x=304, y=164)
            self.sleep2stop()
            ag.doubleClick()
            self.sleep2stop(0.5)

            send_keys(price_)
            self.sleep2stop(0.5)

        # buy
        if buy_:
            # 买入数量: Point(x=313, y=220)，
            # 319, y=218
            ag.moveTo(x=320, y=196)
            self.sleep2stop()

            ag.doubleClick()
            self.sleep2stop()

            send_keys(num_)
            self.sleep2stop()

            # 确认买入: Point(x=343, y=243)，
            ag.moveTo(x=343, y=222)
            self.sleep2stop()

            ag.click()
            self.sleep2stop()

            # 确认提示: Point(x=632, y=441)（回车）
            ag.moveTo(x=633, y=458)
            self.sleep2stop()

            ag.click()
            self.sleep2stop()

            # 确认委托: Point(x=644, y=458)（回车） # x=638, y=463
            ag.moveTo(x=633, y=458)
            self.sleep2stop()

            ag.click()
            self.sleep2stop()

            send_keys('{ENTER}')
            self.sleep2stop()

        else:  # 卖出
            # 买入数量: Point(x=313, y=220)，
            # 319, y=218
            ag.moveTo(x=319, y=218)
            self.sleep2stop(0.5)

            ag.doubleClick()
            self.sleep2stop(0.5)

            send_keys(num_)
            self.sleep2stop()

            # 确认买入: Point(x=343, y=243)，
            ag.moveTo(x=343, y=243)
            self.sleep2stop(0.5)

            ag.click()
            self.sleep2stop(0.5)

            # 确认提示: Point(x=632, y=441)（回车）
            ag.moveTo(x=631, y=442)
            self.sleep2stop(0.5)

            ag.click()
            self.sleep2stop()

            # 确认委托: Point(x=644, y=458)（回车） # x=638, y=463
            ag.moveTo(x=638, y=463)
            self.sleep2stop()

            ag.click()
            self.sleep2stop(1)

            send_keys('{ENTER}')
            self.sleep2stop()

    def buy(self, code_, num_, price_=None):

        self.sleep2stop(1)

        ag.moveTo(x=1100, y=200)
        ag.click()
        self.sleep2stop()

        send_keys('{F1}')
        self.sleep2stop()

        self.actions(code_, num_, price_)

    def sell(self, code_, num_, price_=None):

        self.sleep2stop(1)

        ag.moveTo(x=1100, y=200)
        ag.click()
        self.sleep2stop()

        send_keys('{F2}')
        self.sleep2stop()

        self.actions(code_, num_, price_, buy_=False)


class TradePoolFaker:
    """ 买入数量计算， 持股金额约 5000， 大约5000只持有一手；"""

    @classmethod
    def buy_num(cls, close):
        num_ = 5000 / close

        if num_ < 100:
            num_ = 100

        if num_ > 100:
            num_ = (num_ // 100) * 100 + 100

        return int(num_)

    @classmethod
    def buy_pool(cls):
        pool = StockPoolData.load_StockPool()
        pool = pool[['id', 'name', 'code', 'Classification', 'Industry', 'RnnModel', 'close', 'Position', 'BoardBoll']]
        # print(pool)
        pool = pool[(pool['RnnModel'] < -5) &
                    (~pool['Classification'].isin(['创业板', '科创板'])) &
                    (pool['close'] < 200) &
                    (~pool['Industry'].isin(['房地产', '煤炭采选'])) &
                    (pool['Position'] == 0) & (pool['BoardBoll'].isin([1, 2]))
                    ].sort_values(by=['RnnModel'])
        print(pool)
        # exit()
        trade_ = TongHuaShunAutoTrade()
        for i in pool.index:
            code_ = pool.loc[i, 'code']
            close = pool.loc[i, 'close']
            num_ = cls.buy_num(close)
            price_ = ''
            trade_.buy(code_=code_, num_=str(num_))

        print(f'Buy Succeed;')

    @classmethod
    def sell_pool(cls):
        position = StockPoolData.load_StockPool()
        position = position[(position['Position'] == 1) & (position['TradeMethod'] == 1)]

        print(position.head())
        # exit()
        trade_ = TongHuaShunAutoTrade()

        for index in position.index:
            code_ = position.loc[index, 'code']
            num_ = str(position.loc[index, 'PositionNum'])
            price_ = ''
            trade_.sell(code_, num_)

        print(f'Sell Succeed;')


class TradePoolReal:

    """
    买入数量计算， 持股金额约 5000， 大约5000只持有一手；

    """

    def buy_num(self, close):

        num_ = 5000 / close

        if num_ < 100:
            num_ = 100

        if num_ > 100:
            num_ = (num_ // 100) * 100 + 100

        return int(num_)

    def bottom_down_data(self):

        data_ = StockPoolData.load_StockPool()

        data_ = data_[(data_['Trends'] == -1) &
                      (data_['RnnModel'] < -4.5)]
        return data_

    def bottom_up_data(self):
        data_ = StockPoolData.load_StockPool()
        data_ = data_[(data_['Trends'] == 1) &
                      (data_['RnnModel'] <= 1.5)]
        return data_

    def position_data(self):

        data_ = StockPoolData.load_StockPool()
        data_ = data_[(data_['Position'] == 1) &
                      (data_['TradeMethod'] <= 1)]
        return data_

    def buy_pool(self):
        print(f'Buy Succeed;')
        pass

    def sell_pool(self):
        print(f'Sell Succeed;')
        pass


if __name__ == '__main__':

    code = '002475'
    num = '300'
    price = '10'

    trade = TradePoolFaker()
    # trade.buy_pool()
    trade.sell_pool()
