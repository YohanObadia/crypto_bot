import krakenex
from time import sleep
import pickle
import sys
import multiprocessing as mp
import time
from pprint import pprint

CRYPTO_CURRENCIES_TO_TRADE = ['DASH', 'GNO', 'ETC', 'ETH', 'LTC', 'REP', 'XBT', 'XLM', 'XRP']


class KrakenTrading(object):
    """Create a trading object that query """

    def __init__(self, kraken_key, currencies_history=None):
        self.con = krakenex.API()
        self.kraken_key = kraken_key
        if currencies_history is None:
            self.currencies_history = self.init_currencies_hirstory()
        else:
            self.currencies_history = currencies_history
        self.get_connection()

    @staticmethod
    def init_currencies_hirstory():
        history_template = {'unix_time': mp.Manager().list(),
                            'ask_price': mp.Manager().list(),
                            'bid_price': mp.Manager().list(),
                            'last_trade_price': mp.Manager().list(),
                            'max_price_today': mp.Manager().list(),
                            'max_price_24_hours': mp.Manager().list(),
                            'min_price_today': mp.Manager().list(),
                            'min_price_24_hours': mp.Manager().list(),
                            'today_open_price': mp.Manager().list(),
                            'number_of_trades_today': mp.Manager().list(),
                            'number_of_trades_24_hours': mp.Manager().list(),
                            'volume_weighted_avg_price_today': mp.Manager().list(),
                            'volume_weighted_avg_price_24_hours': mp.Manager().list(),
                            'volume_of_trades_today': mp.Manager().list(),
                            'volume_of_trades_24_hours': mp.Manager().list()}
        currencies_history = {}
        for currency in CRYPTO_CURRENCIES_TO_TRADE:
            currencies_history[currency] = history_template
        return currencies_history

    def get_connection(self):
        self.con.load_key(self.kraken_key)

    def get_ticker_crypto_usd(self, currency):
        try:
            time = self.con.query_public('Time')
            ticker = self.con.query_public('Ticker', {'pair': currency + 'USD'})
            self.currencies_history[currency]['unix_time'].append(time['result']['unixtime'])
            pair = [x for x in ticker['result'].keys()][0]
            self.currencies_history[currency]['ask_price'].append(ticker['result'][pair]['a'][0])
            self.currencies_history[currency]['bid_price'].append(ticker['result'][pair]['b'][0])
            self.currencies_history[currency]['last_trade_price'].append(ticker['result'][pair]['c'][0])
            self.currencies_history[currency]['max_price_24_hours'].append(ticker['result'][pair]['h'][1])
            self.currencies_history[currency]['max_price_today'].append(ticker['result'][pair]['h'][0])
            self.currencies_history[currency]['min_price_24_hours'].append(ticker['result'][pair]['l'][1])
            self.currencies_history[currency]['min_price_today'].append(ticker['result'][pair]['l'][0])
            self.currencies_history[currency]['today_open_price'].append(ticker['result'][pair]['o'])
            self.currencies_history[currency]['volume_weighted_avg_price_today'].append(ticker['result'][pair]['p'][0])
            self.currencies_history[currency]['volume_weighted_avg_price_24_hours'].append(
                ticker['result'][pair]['p'][1])
            self.currencies_history[currency]['number_of_trades_today'].append(ticker['result'][pair]['t'][0])
            self.currencies_history[currency]['number_of_trades_24_hours'].append(ticker['result'][pair]['t'][1])
            self.currencies_history[currency]['volume_of_trades_today'].append(ticker['result'][pair]['v'][0])
            self.currencies_history[currency]['volume_of_trades_24_hours'].append(ticker['result'][pair]['v'][1])
        except Exception as e:
            print(currency)
            pprint(ticker)
            print(e)

    def get_all_ticker_crypto_usd(self):
        with mp.Pool(processes=len(CRYPTO_CURRENCIES_TO_TRADE)) as pool:
            threads = [pool.apply_async(func=self.get_ticker_crypto_usd, args=(currency, )) for currency in CRYPTO_CURRENCIES_TO_TRADE]
            for p in threads:
                try:
                    p.get()
                except Exception as e:
                    print('get exception: ')
                    print(e)
        with open('currencies_history.p', 'wb') as f:
            pickle.dump(self.currencies_history, f)

    def get_ticker_each_minute(self):
        counter = 0
        try:
            while True:
                print('Start loop...')
                start_time = time.time()
                self.get_all_ticker_crypto_usd()
                print("--- %s seconds ---" % (time.time() - start_time))
                counter += 1
                print('End loop number ' + str(counter))
                sleep(60)
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    with open('currencies_history.p', 'rb') as f:
        currencies_history = pickle.load(f)
    kraken_trading = KrakenTrading('kraken.key', currencies_history)
    kraken_trading.get_ticker_each_minute()