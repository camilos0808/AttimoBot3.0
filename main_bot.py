from util.binance_API import futures_klines_df, last_tickers, futures_symbol_list
import util.telegram as tlg
from ta.momentum import rsi
import datetime as dt
import time
import math
import pandas as pd
import numpy as np
import os
import pathlib

'''
AUXILIARY METHODS
'''


def calculator(data):
    data['RSI'] = rsi(data.close)
    data = data.loc[data['timestamp'] > data['timestamp'].max() - 20 * dt.timedelta(minutes=30)].copy()
    avg = data.close.rolling(20).mean()
    std = data.close.rolling(20).apply(np.std)
    hband = avg + 2 * std
    lband = avg - 2 * std
    data['bbh_ind'] = (data.close > hband).astype(int)
    data['bbl_ind'] = (data.close < lband).astype(int)
    data = data.loc[data.symbol.shift(20 - 1) == data.symbol].copy()

    return data


def download_db(symbols, klines):
    data_base = futures_klines_df(symbols, klines, 300)
    data_base['bbh_ind'] = 0
    data_base['bbl_ind'] = 0
    data_base['RSI'] = 50
    return data_base


def sleep_time():
    sleep_min = round((math.ceil(dt.datetime.now().minute / 30) - dt.datetime.now().minute / 30) * 30)
    if sleep_min > 1:
        time.sleep((sleep_min - 1) * 60)
        print('DONE SLEEPING')


def correct_db(data_db):
    last_price = last_tickers(data_db.timestamp.max())
    last_price['close'] = last_price['close'].astype(float)
    index_coin = last_price.sort_values('close', ascending=False).reset_index(drop=True).reset_index()[
        ['index', 'symbol']]
    combine = data_db.merge(last_price, on=['timestamp', 'symbol'], how='left', suffixes=('_', ''))
    combine['close'] = combine['close'].fillna(combine['close_']).astype(float)
    combine = combine.drop('close_', axis=1)
    combine = pd.merge(combine, index_coin, on=['symbol'])
    combine = combine.sort_values(['index', 'timestamp'])

    return combine


def message(exchange, symbol, rsi):
    msg = '📢️ *ALERTA*\n ----------------------------\n'
    wallet = 'Las siguientes criptomonedas en *BINANCE {}* presentan un volumen que ha activado una alerta ' \
             'de _RSI_. *Operar con precaución*:\n\n'.format(exchange)
    msg = msg + wallet

    if rsi < 50:
        msg += '*Oportunidad de Largo*\n'
    else:
        msg += '*Oportunidad de Corto*\n'

    msg += 'RSI: *{}* --> ${}\n'.format(rsi, symbol)

    return msg
'''
CLASSES
'''


class BollBOT:
    INIT = os.path.join(pathlib.Path().absolute())

    def __init__(self, klines, symbol_list, rsi_limits, max_in, long=True, short=True):
        self.klines = klines
        self.symbol_list = symbol_list
        self.to_trade = {}
        self.historic = True
        self.rsi_limits = rsi_limits
        self.max_in = max_in
        self.long = long
        self.short = short
        self.sl_activation = False
        self.db = download_db(symbol_list, self.klines)

    def init(self):
        while True:
            actual_minute = dt.datetime.now().minute
            if not self.historic and actual_minute not in [0, 30]:
                self.db = download_db(self.symbol_list, self.klines)
                self.historic = True
                sleep_time()
            elif self.historic and actual_minute in [0, 30]:
                self.db = correct_db(self.db)
                self.db = calculator(self.db)
                self.to_trade = self.triggered_symbols()
                if self.to_trade.__len__() != 0:
                    for symbol in self.to_trade.keys():
                        tlg.send_message(message('BINANCE FUTURES', symbol, self.to_trade[symbol]['RSI']))
                else:
                    print('NO COINS')
                self.historic = False

    def triggered_symbols(self):
        symbols = {}
        max_klines = self.db.timestamp.max()

        last_klines = self.db.loc[self.db['timestamp'] == max_klines].copy()
        last_klines.loc[(last_klines['bbh_ind'] == 1) & (last_klines['RSI'] > self.rsi_limits[1]), 'side'] = -1
        last_klines.loc[(last_klines['bbl_ind'] == 1) & (last_klines['RSI'] < self.rsi_limits[0]), 'side'] = 1
        last_klines.dropna(inplace=True)
        if last_klines.__len__() == 0:
            return symbols
        last_klines.loc[(last_klines['RSI'] > 50), 'RSI_IND'] = 100 - last_klines['RSI']
        last_klines.loc[(last_klines['RSI'] < 50), 'RSI_IND'] = last_klines['RSI']
        if not self.short:
            last_klines = last_klines.loc[last_klines['side'] != -1].copy()
        elif not self.long:
            last_klines = last_klines.loc[last_klines['side'] != 1].copy()

        last_klines = last_klines.sort_values('RSI_IND').head(self.max_in)

        for index, row in last_klines.iterrows():
            symbols[row.symbol] = {
                'RSI': row.RSI
            }

        return symbols


'''
BOT INIT
'''
symbol_list = futures_symbol_list()

symbol_list = [x for x in symbol_list if x.endswith('USDT')]
symbol_list = [x for x in symbol_list if not x == 'DOGEUSDT']
symbol_list = [x for x in symbol_list if not x == '1000SHIBUSDT']
symbol_list = [x for x in symbol_list if not x == 'BTCSTUSDT']
klines = '30m'

rsi_limit = [25, 78]

max_in = 1

instance = BollBOT(klines, symbol_list, rsi_limit, 1)
instance.init()
