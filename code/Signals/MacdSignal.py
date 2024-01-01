# -*- coding: utf-8 -*-
from code.parsers.MacdParser import *


def calculate_MACD(data, ema_short=12, ma_mid=20, ema_long=30, MID=9):

    # 60num_mins MACD 计算 , 需要计算的几个数据 1. ema_short = 12 , ema_long = 26 , MID=9

    data.loc[:, EmaShort] = data['close'].rolling(ema_short, min_periods=1).mean()
    data.loc[:, EmaMid] = data['close'].rolling(ma_mid, min_periods=1).mean()
    data.loc[:, EmaLong] = data['close'].rolling(ema_long, min_periods=1).mean()

    data.loc[:, Dif] = data[EmaShort] - data[EmaLong]
    data.loc[:, DifSm] = data[EmaShort] - data[EmaMid]
    data.loc[:, DifMl] = data[EmaMid] - data[EmaLong]

    data.loc[:, Dea] = data[Dif].rolling(MID, min_periods=1).mean()
    data.loc[:, macd_] = (data[Dif] - data[Dea]) * 2

    return data
