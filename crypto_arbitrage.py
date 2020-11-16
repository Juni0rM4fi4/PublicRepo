import re
import math
import time
import ccxt
import logging
import pandas as pd
from keys import *
from itertools import permutations

######################################
#########  TO MANUALLY SET  ##########
######### - file_handler    ##########
######### - API_KEY         ##########
######### - SECRET_KEY      ##########
######################################

my_logger = logging.getLogger(__name__)
my_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('/Users/trades.txt', mode='a')
my_logger.addHandler(file_handler)


class CryptocurrencyTradingBot:

    def __init__(self, exchange_id):
        self.symbol_to_arbitrage = ''
        self.exchanges = ccxt.exchanges
        self.exchange_id = exchange_id
        self.taker_fee = 0
        self.bid_asks = {}  #Order Book for triangles
        self.pair_info = {}

    def style(self, s, style):
        return style + s + '\033[0m'

    def green(self, s):
        return self.style(s, '\033[92m')

    def red(self, s):
        return self.style(s, '\033[91m')

    def contains(self, target, string):
        if len(re.findall(rf'^{target}/', string)) > 0 or len(re.findall(rf'/{target}$', string)) > 0:
            return True
        else:
            return False

    def to_ask_or_to_bid(self, x):
        to_ask_or_to_bid = []
        to_ask_or_to_bid.append('bid') if x.First.find(self.symbol_to_arbitrage) == 0 else to_ask_or_to_bid.append('ask')
        to_ask_or_to_bid.append('bid') if x.Second.find(x.First.replace(self.symbol_to_arbitrage, '').replace('/', '')) == 0 else to_ask_or_to_bid.append('ask')
        to_ask_or_to_bid.append('ask') if x.Third.find(self.symbol_to_arbitrage) == 0 else to_ask_or_to_bid.append('bid')
        return to_ask_or_to_bid

    def x_to_y(self, ask_or_bid, orderbook, min_amount):
        print(f"{ask_or_bid} ({min_amount}): {orderbook}")

        exchanged = []
        exchanged_for = []

        for i, price_and_size in enumerate(orderbook):

            exchanged.append(price_and_size[0] * price_and_size[1] if ask_or_bid == 'ask' else price_and_size[1])

            if exchanged[i] > min_amount:
                exchanged.pop()
                exchanged.append(min_amount - sum(exchanged))
                exchanged_for.append(exchanged[i] / price_and_size[0] if ask_or_bid == 'ask' else exchanged[i] * price_and_size[0])
                break
            else:
                exchanged_for.append(exchanged[i] / price_and_size[0] if ask_or_bid == 'ask' else exchanged[i] * price_and_size[0])

        print(exchanged)
        print(exchanged_for)

        return sum(exchanged_for)

    def match_bidask(self, x):

        try:

            to_ask_or_to_bid = self.to_ask_or_to_bid(x)

            checkpoints, prices, mins = [], [], []

            for i, pair in enumerate(x):
                price = self.bid_asks[pair][to_ask_or_to_bid[i]]
                prices.append(price)
                checkpoints.append(price) if to_ask_or_to_bid[i] == 'bid' else checkpoints.append(1/price)

            dummy_return = checkpoints[0]*checkpoints[1]*checkpoints[2]

            if dummy_return > 1:  # If theoretically profitable (without consideration of the orderbook)
                print(f"Looks profitable: {list(x), to_ask_or_to_bid, prices, (dummy_return - 1) * 100}")
                return [list(x), to_ask_or_to_bid, prices, (dummy_return - 1) * 100]
            else:
                return []

        except Exception as e:
            print(f"Error: {e}:{[to_ask_or_to_bid, prices, list(x)]}")
            return []

    def initializer(self):

        print(f"\n{len(self.exchanges)} exchanges available with ccxt\n")

        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            exchange = exchange_class({
                'apiKey': API_KEY_BITTREX,
                'secret': SECRET_KEY_BITTREX,
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

    def triangles_builder(self, my_exchange):

        print(f"\nSymbol to arbitrage: {self.symbol_to_arbitrage}\n")

        print(f"Filtering only live pairs")

        fetchTickers_dict = my_exchange.fetchTickers(my_exchange.symbols)
        fetchTickers_df = pd.DataFrame(fetchTickers_dict.values(), index=fetchTickers_dict.keys())
        fetchTickers_df = fetchTickers_df[(fetchTickers_df.bid.notnull()) & (fetchTickers_df.ask.notnull()) & (fetchTickers_df.ask != 0)]

        pairs_live = list(fetchTickers_df.symbol)

        print(f"{len(pairs_live)} live pairs ({len(my_exchange.symbols)-len(pairs_live)} not working)")

        pairs_to_arbitrage = list(filter(lambda x: self.contains(self.symbol_to_arbitrage, x), pairs_live))  # Filters pairs containing self.symbol_to_arbitrage
        print(f"{len(pairs_to_arbitrage)} relevant live pairs containing {self.symbol_to_arbitrage}")

        pairs_for_triangle = list(map(lambda x: '/'.join(x), list(permutations(list(map(lambda x: x.replace(self.symbol_to_arbitrage, '').replace('/', ''), pairs_to_arbitrage)), 2))))  # Removes 'BTC' and '/' from pairs, then create every combinations (n-1)*n
        print(f"{len(pairs_for_triangle)} combinations of possible mate")

        pairs_available = list(filter(lambda x: True if x in pairs_live else False, pairs_for_triangle))  # Filters pairs actually available in exchange
        print(f"{len(pairs_available)} available combinations of possible mate")

        triangles = pd.DataFrame(columns=['First', 'Second', 'Third'])

        for pairs in pairs_to_arbitrage:
            mates = list(filter(lambda x: self.contains(pairs.replace(self.symbol_to_arbitrage, '').replace('/', ''), x), pairs_available))
            for mate in mates:
                anchors = list(filter(lambda x: self.contains(mate.replace(pairs.replace(self.symbol_to_arbitrage, '').replace('/', ''), '').replace('/', ''), x), pairs_to_arbitrage))
                for anchor in anchors:
                    triangles = triangles.append({'First': pairs, 'Second': mate, 'Third': anchor}, ignore_index=True)

        print(f"{len(triangles)} possible triangles with {self.symbol_to_arbitrage}")

        return triangles

    def routine(self, symbol):
        my_exchange = self.initializer()
        self.symbol_to_arbitrage = symbol

        if my_exchange is not None:

            while True:

                triangles = self.triangles_builder(my_exchange)

                if len(triangles) > 0:
                    unique_pairs = list(set(list(triangles.First) + list(triangles.Second) + list(triangles.Third)))

                    print(f"Fetching amount precision and minimum amount for {len(unique_pairs)} unique pairs")
                    amount_precision = list(map(lambda x: my_exchange.markets[x]['precision']['amount'], unique_pairs))
                    amount_min = list(map(lambda x: my_exchange.markets[x]['limits']['amount']['min'], unique_pairs))
                    self.pair_info = dict((z[0], list(z[1:])) for z in zip(unique_pairs, amount_precision, amount_min))

                    print(f"\nFetching Bid-Ask for {len(unique_pairs)} unique pairs...")
                    start_time = time.time()
                    self.bid_asks = (my_exchange.fetchTickers(unique_pairs))
                    print(f"Fetching took {round(time.time() - start_time, 2)}s\n")

                    live_triangles = triangles.apply(self.match_bidask, axis=1)

                    profitable_triangles = list(filter(lambda x: True if len(x) > 0 else False, live_triangles))

                    for triangle in profitable_triangles:
                        print(triangle)

                        balance = self.x_to_y(triangle[1][0], my_exchange.fetchOrderBook(triangle[0][0])[str(triangle[1][0] + 's')], self.pair_info[triangle[0][0]][1])
                        balance = self.x_to_y(triangle[1][1], my_exchange.fetchOrderBook(triangle[0][1])[str(triangle[1][1] + 's')], balance * (1-self.taker_fee))
                        balance = self.x_to_y(triangle[1][2], my_exchange.fetchOrderBook(triangle[0][2])[str(triangle[1][2] + 's')], balance * (1-self.taker_fee))

                        profit = (((balance * (1-self.taker_fee)) / self.pair_info[triangle[0][0]][1])-1)*100

                        if 0 < profit < 1:
                            print(self.green(f"{'!'*20} Profit ({profit}) {'!'*20}"))
                            params = {'test': True}

                            my_logger.info(f"{profit} // {triangle}")

                            break
                        else:
                            print(self.red(f"No profit ({profit})"))

                        print("\n")

                else:
                    print("No triangles to arbitrage from.")

        else:
            print("Aborted")


if __name__ == '__main__':

    bot = CryptocurrencyTradingBot(exchange_id='binance')
    bot.routine(symbol='BTC')
