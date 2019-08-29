# coding: utf-8
import numpy as np

from abc import abstractmethod

from trade.core import Trader
from trade.kline import Bar


class BaseStrategy(Trader):
    def __init__(self, host="https://www.ybmex.com",
                 symbol="XBTUSD", api_key="", api_secret="",
                 kline_opts=None):
        super(BaseStrategy, self).__init__(host, symbol, api_key, api_secret)

        default_kline_opts = {
            "precise": self.kline.DEFAULT_PRECISE,
            "count": 100,
            "from_ts": None,
            "to_ts": None,
            "endpoint": "/history",
            "mode": "first",
            "trigger": False
        }

        if kline_opts:
            default_kline_opts.update(kline_opts)

        self.kline.retrieve_bars(**default_kline_opts)

        self._close_price = np.array(
            [b.close for b in self.kline.history])

        self._position = None
        self._profit = list()

    @abstractmethod
    def on_bar(self, bar: Bar):
        self._close_price = np.append(self._close_price, bar.close)

    @abstractmethod
    def on_trade(self, trade: dict):
        pass
