# coding: utf-8
from threading import Event
from pprint import pprint
from collections import defaultdict, OrderedDict

from clients.nge_rest import api
from clients.nge_websocket import NGEWebsocket

from trade.kline import Bar, Kline


class Trader(NGEWebsocket):
    is_test = False

    MAX_KLINE_LEN = 1000

    def __init__(self, host="https://www.ybmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        self._host = host
        self._api_key = api_key
        self._api_secret = api_secret

        self._running_flag = Event()

        self.kline = Kline(host=self._host, symbol=symbol,
                           bar_callback=self.on_bar,
                           running=self._running_flag)

        self._order_cache = defaultdict(OrderedDict)

        self._rest_client = api(host=self._host,
                                api_key=self._api_key,
                                api_secret=self._api_secret)

        super(Trader, self).__init__(host=self._host,
                                     symbol=symbol,
                                     api_key=self._api_key,
                                     api_secret=self._api_secret)
        self.running = True

    @property
    def running(self):
        return self._running_flag.is_set()

    @running.setter
    def running(self, value):
        if value:
            self._running_flag.set()
        else:
            self._running_flag.clear()

    def exit(self):
        super(Trader, self).exit()

        self.running = False

    def join(self):
        self.wst.join()

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

    def partial_handler(self, table_name, message):
        super(Trader, self).partial_handler(table_name, message)

        if table_name == "trade":
            for trade in self.recent_trades():
                self.kline.notify_trade(trade)

    def insert_handler(self, table_name, message):
        super(Trader, self).insert_handler(table_name, message)

        if table_name == "trade":
            for trade in message["data"]:
                self.kline.notify_trade(trade)
