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
import json

'''
AUXILIARY METHODS
'''


def calculator(data):
    data['RSI'] = rsi(data.close)
    data['RSI_6'] = rsi(data.close, n=6)
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
        time.sleep((sleep_min - 0.8) * 60)
        # print('DONE SLEEPING')


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


def message(exchange, symbol, rsi, price, price_bool=False):
    msg = '📢️ *ALERTA*\n ----------------------------\n'

    msg += 'Oportunidad en *{}*:\n\n'.format(exchange)

    if rsi < 50:
        tipo = '📈 LARGO'
    else:
        tipo = '📉 CORTO'

    if price_bool:
        wallet = 'Moneda: ${}\nTipo: {}\nPrecio Referencia: {}\n\n'.format(symbol, tipo, price)
    else:
        wallet = 'Moneda: ${}\nTipo: {}\n\n'.format(symbol, tipo)

    msg = msg + wallet

    msg += 'Recuerda *Operar con precaución*. El mercado de las criptomonedas puede llegar a ser muy volatil. Controla ' \
           'tu riesgo con un STOP LOSS. \n\n '

    pst = '*Disclaimer:* \nAttimoBOT no asume responsabilidad alguna, explícita o ' \
          'implícita, directa o indirecta, por los daños producidos por el uso de la información suministrada en este ' \
          'mensaje, ' \
          'así como en las conferencias, eventos, charlas, sesiones de consultoría y/o similares organizados y/o ' \
          'brindados. Perseguimos fines únicamente educativos. La información y los ejemplos brindadosno deben ' \
          'interpretarse como una promesa o garantía de ganancias. '

    # pst = '*Descargo de responsabilidad* \n  Tenga en cuenta que soy propietario de una cartera diversificada, ' \
    #       'ya que deseo ser transparente e imparcial para la comunidad de AttimoCrypto en todo momento y, ' \
    #       'por lo tanto, el contenido de mis medios está destinado A FINES DE INFORMACIÓN GENERAL. La información ' \
    #       'aquí contenida es solo para fines informativos. Nada del contenido de dicho canal y/o respuestas en ' \
    #       'comentarios se interpretará como asesoramiento financiero, legal o fiscal. El tema tratado en cada video ' \
    #       'es únicamente la opinión subjetiva del orador que no es un asesor financiero con licencia o un asesor de ' \
    #       'inversiones registrado. La compra de criptomonedas, operar en el mercado financiero plantea un riesgo ' \
    #       'considerable de pérdida. El orador no garantiza ni se responsabiliza por ningún resultado en particular. ' \
    #       'El rendimiento pasado no indica resultados futuros. Esta información es la que se encuentra públicamente ' \
    #       'en Internet. Toda la información está destinada a la conciencia pública y es de dominio público. Puede ' \
    #       'tomar estos datos y argumentos y hacer su propia investigación. '

    msg += pst

    return msg


'''
CLASSES
'''


class BollBOT:
    INIT = os.path.join(pathlib.Path().absolute())

    def __init__(self, klines, symbol_list, rsi_limits, max_in, price_bool=False, long=True, short=True):
        self.price_bool = price_bool
        self.klines = klines
        self.symbol_list = symbol_list
        self.to_trade = {}
        self.historic = True
        self.rsi_limits = rsi_limits
        self.max_in = max_in
        self.long = long
        self.short = short
        self.sl_activation = False
        self.trade_dir = os.path.join(BollBOT.INIT, 'calls.json')
        self.db = download_db(symbol_list, self.klines)
        self.calls = self.load()
        sleep_time()

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
                        tlg.send_message(message('BINANCE FUTURES',
                                                 symbol,
                                                 self.to_trade[symbol]['RSI'],
                                                 self.to_trade[symbol]['price'],
                                                 self.price_bool))
                        self.add(symbol, self.to_trade[symbol]['RSI'])
                        self.save()
                # else:
                #     print('NO COINS')
                self.historic = False

    def triggered_symbols(self):
        symbols = {}
        max_klines = self.db.timestamp.max()

        last_klines = self.db.loc[self.db['timestamp'] == max_klines].copy()
        last_klines.loc[(last_klines['bbh_ind'] == 1) & (last_klines['RSI'] > self.rsi_limits[1]) & (last_klines['RSI_6'] > 90), 'side'] = -1
        last_klines.loc[(last_klines['bbl_ind'] == 1) & (last_klines['RSI'] < self.rsi_limits[0]) & (last_klines['RSI_6'] < 10), 'side'] = 1
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
                'RSI': row.RSI,
                'price': row.close,
            }

        return symbols

    def save(self):
        with open(self.trade_dir, 'w') as fp:
            json.dump(self.calls, fp)

    def load(self):
        if os.path.exists(self.trade_dir):
            with open(self.trade_dir, 'r') as fp:
                data = json.load(fp)
        else:
            data = {}

        return data

    def add(self, symbol, rsi):
        if self.calls.__len__() == 15:
            try:
                del self.calls[1]
            except:
                del self.calls['1']
            self.calls = {i + 1: v for i, v in enumerate(self.calls.values())}
            self.calls[self.calls.__len__() + 1] = {
                'symbol': symbol,
                'timestamp': dt.datetime.now().isoformat(),
                'rsi': rsi}
        else:
            self.calls[self.calls.__len__() + 1] = {
                'symbol': symbol,
                'timestamp': dt.datetime.now().isoformat(),
                'rsi': rsi}


'''
BOT INIT
'''
symbol_list = futures_symbol_list()

symbol_list = [x for x in symbol_list if x.endswith('USDT')]
symbol_list = [x for x in symbol_list if not x == 'DOGEUSDT']
symbol_list = [x for x in symbol_list if not x == '1000SHIBUSDT']
symbol_list = [x for x in symbol_list if not x == 'BTCSTUSDT']
symbol_list = [x for x in symbol_list if not x == 'BTCDOMUSDT']
klines = '30m'

rsi_limit = [19, 83]

max_in = 1
price_bool = True

instance = BollBOT(klines, symbol_list, rsi_limit, 1, price_bool)
instance.init()
