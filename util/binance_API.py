from binance.client import Client
import pandas as pd
import time

client = Client()
seconds = 30


def futures_klines_df(symbols, kline_size, ma):
    data = pd.DataFrame()
    for symbol in symbols:
        try:
            klines = client.futures_klines(symbol=symbol, interval=kline_size, limit=ma)
        except:
            time.sleep(seconds)
            klines = client.futures_klines(symbol=symbol, interval=kline_size, limit=ma)

        new_data = pd.DataFrame(klines,
                                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                         'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
        new_data['symbol'] = symbol
        if new_data.__len__() >= ma:
            if len(data) == 0:
                data = new_data
            else:
                data = data.append(new_data)
                data.reset_index(inplace=True, drop=True)

    data['close'] = data['close'].astype(float)
    data['timestamp'] = data['timestamp'] - 5 * 60 * 60 * 1000
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')

    return data


def last_tickers(last_kline):
    try:
        df = pd.DataFrame(client.futures_ticker())[['symbol', 'lastPrice']]
    except:
        time.sleep(seconds)
        df = pd.DataFrame(client.futures_ticker())[['symbol', 'lastPrice']]
    df['timestamp'] = last_kline
    df.columns = ['symbol', 'close', 'timestamp']

    return df


def futures_symbol_list():
    try:
        symbol_info = client.futures_exchange_info()['symbols']
    except:
        time.sleep(seconds)
        symbol_info = client.futures_exchange_info()['symbols']

    df = pd.DataFrame(symbol_info)

    return df['symbol'].tolist()
