# coding: utf-8
import time

from pprint import pprint
from collections import defaultdict, OrderedDict

from clients.nge_rest import api
from clients.nge_websocket import NGEWebsocket

from trade.kline import Bar, Kline


class Trader(NGEWebsocket):
    is_test = False

    MAX_KLINE_LEN = 1000

    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        self._host = host
        self._api_key = api_key
        self._api_secret = api_secret

        self._order_cache = defaultdict(OrderedDict)

        self._rest_client = api(host=self._host,
                                api_key=self._api_key,
                                api_secret=self._api_secret)

        super(Trader, self).__init__(host=self._host,
                                     symbol=symbol,
                                     api_key=self._api_key,
                                     api_secret=self._api_secret)

        self.kline = Kline(host=self._host, symbol=symbol,
                           bar_callback=self.on_bar,
                           running=self._running_flag)

        self.running = True

    @property
    def buy_side(self):
        buy_side = [o for o in self.data["orderBookL2"] if o["side"] == "Buy"]
        buy_side.sort(key=lambda o: o["price"], reverse=True)

        return buy_side

    @property
    def best_buy(self):
        return self.buy_side[0]

    @property
    def sell_side(self):
        sell_side = [o for o in self.data["orderBookL2"] if o["side"] == "Sell"]
        sell_side.sort(key=lambda o: o["price"])

        return sell_side

    @property
    def best_sell(self):
        return self.sell_side[0]

    @property
    def best_quote(self):
        return self.best_sell, self.best_buy

    def join(self):
        while self.running:
            if self.wst and self.wst.is_alive():
                self.wst.join()
            else:
                time.sleep(0.5)

    def on_tick(self, tick_data):
        pass

    def on_trade(self, trade_data):
        pass

    def on_quote(self, quote_data):
        pass

    # noinspection PyMethodMayBeStatic
    def on_bar(self, bar: Bar):
        pprint(bar)

    def on_rtn_order(self, order):
        pass

    def on_rtn_trade(self, trade):
        pass

    def _partial_handler(self, table_name, message):
        super(Trader, self)._partial_handler(table_name, message)

        if table_name == "trade":
            for trade_data in self.recent_trades():
                self.kline.notify_trade(trade_data)

    def _insert_handler(self, table_name, message):
        super(Trader, self)._insert_handler(table_name, message)

        if table_name == "trade":
            for trade_data in message["data"]:
                # sequence can not be revered
                try:
                    self.on_trade(trade_data=trade_data)
                except Exception as e:
                    self.logger.exception(e)

                self.kline.notify_trade(trade_data)

        if table_name == "order":
            for order_data in message["data"]:
                try:
                    self.on_rtn_order(order_data)
                except Exception as e:
                    self.logger.exception(e)

        if table_name == "execution":
            for exe in message["data"]:
                try:
                    self.on_rtn_trade(exe)
                except Exception as e:
                    self.logger.exception(e)

    def _update_handler(self, table_name, message):
        super(Trader, self)._update_handler(table_name, message)

        if table_name == "order":
            for order_data in message["data"]:
                try:
                    self.on_rtn_order(order_data)
                except Exception as e:
                    self.logger.exception(e)

        if table_name == "execution":
            for exe in message["data"]:
                try:
                    self.on_rtn_trade(exe)
                except Exception as e:
                    self.logger.exception(e)

    def __getattr__(self, item):
        return getattr(self._rest_client, item)


if __name__ == "__main__":
    ybmex = Trader()

    pprint(ybmex.best_quote)
