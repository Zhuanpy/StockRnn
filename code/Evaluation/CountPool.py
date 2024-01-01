from code.MySql.LoadMysql import StockPoolData
import pandas as pd


class PoolCount:

    @classmethod
    def count_trend(cls, date_=None):

        pool = StockPoolData.load_StockPool()

        # values
        if not date_:
            date_ = pool.iloc[0]['RecordDate']

        pool = pool[pool['RecordDate'] == pd.to_datetime(date_)]

        pool = pool[['RecordDate', 'Trends', 'ReTrend', 'RnnModel']].reset_index(drop=True)

        ''' 统计趋势 '''
        pool.loc[pool['Trends'].isin([2, 3]), 'UpDown'] = 1
        pool.loc[pool['Trends'].isin([0, 1]), 'UpDown'] = -1

        UPs = pool[pool['UpDown'] == 1].shape[0]
        ReUp_ = pool[(pool['UpDown'] == 1) & (pool['ReTrend'] == 1)].shape[0]

        DOWNs = pool[pool['UpDown'] == -1].shape[0]
        ReDown_ = pool[(pool['UpDown'] == -1) & (pool['ReTrend'] == 1)].shape[0]

        _down = pool[pool['Trends'] == 0].shape[0]
        down_ = pool[pool['Trends'] == 1].shape[0]
        _up = pool[pool['Trends'] == 2].shape[0]
        up_ = pool[pool['Trends'] == 3].shape[0]

        ''' 统计Rnn得分'''
        up1 = pool[(pool['RnnModel'] > 0) & (pool['RnnModel'] < 2.5)].shape[0]
        up2 = pool[(pool['RnnModel'] >= 2.5) & (pool['RnnModel'] < 5)].shape[0]
        up3 = pool[(pool['RnnModel'] >= 5)].shape[0]

        down1 = pool[(pool['RnnModel'] > -2.5) & (pool['RnnModel'] < 0)].shape[0]
        down2 = pool[(pool['RnnModel'] > -5) & (pool['RnnModel'] <= -2.5)].shape[0]
        down3 = pool[(pool['RnnModel'] <= -5)].shape[0]

        ''' count board '''
        board = StockPoolData.load_board()

        _BoardDown = board[board['Trends'] == 0].shape[0]
        BoardDown_ = board[board['Trends'] == 1].shape[0]
        _BoardUp = board[board['Trends'] == 2].shape[0]
        BoardUp_ = board[board['Trends'] == 3].shape[0]

        ''' values DataFrame '''
        dic = {'date': [date_],
               'Up': [UPs], 'ReUp': [ReUp_], 'Down': [DOWNs], 'ReDown': [ReDown_],
               '_BoardUp': [_BoardUp], 'BoardUp_': [BoardUp_], '_BoardDown': [_BoardDown], 'BoardDown_': [BoardDown_],
               '_up': [_up], 'up_': [up_], '_down': [_down], 'down_': [down_],
               'Up1': [up1], 'Up2': [up2], 'Up3': [up3], 'Down1': [down1], 'Down2': [down2], 'Down3': [down3]}

        data = pd.DataFrame(dic)

        import sqlalchemy

        try:
            StockPoolData.append_poolCount(data)

        except sqlalchemy.exc.IntegrityError:

            sql = f'''update  {StockPoolData.db_pool}.{StockPoolData.tb_poolCount} set 
            Up = '{UPs}', 
            ReUp='{ReUp_}', 
            Down= '{DOWNs}', 
            _up = '{_up}', 
            up_  = '{up_}',
            _down = '{_down}', 
            down_ = '{down_}',
            ReDown= '{ReDown_}', 
            _BoardUp = '{_BoardUp}', 
            BoardUp_='{BoardUp_}', 
            _BoardDown= '{_BoardDown}', 
            BoardDown_= '{BoardDown_}',
            Up1 = '{up1}', 
            Up2 = '{up2}', 
            Up3= '{up3}', 
            Down1 = '{down1}', 
            Down2 = '{down2}', 
            Down3 = '{down3}' 
            where date = '{date_}';'''

            StockPoolData.pool_execute_sql(sql)

        print('Count Pool Trends Success;')

        return data


if __name__ == '__main__':
    count = PoolCount.count_trend()
