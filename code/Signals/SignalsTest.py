import pandas as pd

from code.MySql.DB_MySql import MysqlAlchemy as alc
from code.Normal import ResampleData as rsp
from StatisticsMacd import CountMACD as count
from StatisticsMacd import SignalMethod as sgl

data_1m = alc.pd_read(database='data1m2022', table='002475')
data_1m = data_1m[data_1m['date'] > pd.Timestamp('2022-08-15')]

data_15m = rsp.resample_1m_data(data_1m, '15m')
# data_15m = count.count_MACD(data)

data = sgl.signal_by_MACD_3ema(data_15m, data_1m)
# print(data)
# exit()
data = data[['date', 'high', 'low', 'Signal', 'SignalChoice',
             'SignalTimes', 'SignalStartTime', 'Daily1mVolMax1',
             'StopLoss']]
data = data.dropna(subset=['SignalChoice'])
print(data)

# date   open  close   high    low
# volume       money   EmaShort   EmaMid    EmaLong
# Dif     DifSm     DifMl       Dea      MACD
# Signal SignalChoice   SignalTimes SignalStartTime  Daily1mVolMax1
# Daily1mVolMax5  Daily1mVolMax15  EndPrice       EndPriceIndex  EndDaily1mVolMax5  StartPrice
# StartPriceIndex  CycleAmplitudePerBar  CycleAmplitudeMax  Cycle1mVolMax1  Cycle1mVolMax5  CycleLengthMax
# CycleLengthPerBar  BollMid   BollStd     BollUp     BollDn  StopLoss
