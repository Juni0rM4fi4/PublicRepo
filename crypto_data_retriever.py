import ccxt
import time
import numpy as np
from keys import *
import pandas as pd
from datetime import datetime

######################################
#########  TO MANUALLY SET  ##########
######### - self.save_path  ##########
######### - API_KEY         ##########
######### - SECRET_KEY      ##########
######################################

class Cryptocurrency():

    def __init__(self, exchange, symbol, timeframe):

        self.save_path = "/Users/"
        self.exchanges = ccxt.exchanges
        self.exchange_id = exchange
        self.symbol = symbol
        self.timeframe = timeframe
        self.taker_fee = 0
        self.limit = 960
        self.tf = {'1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200, '1d': 86400, '3d': 259200, '1w': 604800, '1M': 2592000}

    def style(self, s, style):
        return style + s + '\033[0m'

    def green(self, s):
        return self.style(s, '\033[92m')

    def red(self, s):
        return self.style(s, '\033[91m')

    def initializer(self):

        print(f"\n{len(self.exchanges)} exchanges available with ccxt\n")

        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            exchange = exchange_class({
                'apiKey': API_KEY_BINANCE,
                'secret': SECRET_KEY_BINANCE,
                'timeout': 30000,
                'enableRateLimit': True,
            })
            exchange.load_markets()

            self.taker_fee = exchange.markets[exchange.symbols[0]]['taker']

            print(self.green(chr(126) * 100))
            print(f"{chr(32) * 1}{self.green(exchange.id.upper())} (Status: {self.green(exchange.fetchStatus()['status'].upper())}, Required Credentials: {self.green('OK') if exchange.checkRequiredCredentials() is True else self.red('NOK')}, Taker Fee: {self.taker_fee}, Rate Limit: {exchange.rateLimit}ms)")
            print(self.green(chr(126) * 100))
            print(f"\n{len(exchange.symbols)} pairs available: {exchange.symbols}")

            return exchange

        except Exception as e:
            print(f"Error initializing exchange: {e}")
            return None

    def getData(self):

        exchange = self.initializer()

        now = datetime.timestamp(datetime.now())
        startdate = exchange.fetch_ohlcv(self.symbol, self.timeframe, exchange.parse8601('2000-01-01T00:00:00Z'), 1)

        print(f"First date of data: {exchange.iso8601(startdate[0][0])}")

        sinces = np.arange(startdate[0][0], int(now*1000), int(self.limit*self.tf[self.timeframe]*1000))

        header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']

        temp = []
        df_full = pd.DataFrame()
        df_temp = pd.DataFrame()

        for since in sinces:
            temp = []
            temp = exchange.fetch_ohlcv(self.symbol, self.timeframe, int(since), self.limit)
            if len(temp) > 1:
                print(f"[{self.symbol}] Retrieved {len(temp)} datapoints from {datetime.utcfromtimestamp(temp[0][0]/1000).strftime('%Y-%m-%d %H:%M:%S')} to {datetime.utcfromtimestamp(temp[-1][0]/1000).strftime('%Y-%m-%d %H:%M:%S')}")
                df_temp = pd.DataFrame(temp, columns=header).set_index('Timestamp')
                df_temp['Symbol'] = self.symbol
                df_full = pd.concat([df_full, df_temp])
            time.sleep(3)

        df_full.to_csv(f"{self.save_path}/{self.symbol.replace('/','-')}.csv", index=True)

if __name__ == '__main__':

    Cryptocurrency('binance', 'ETH/BTC', '5m').getData()



