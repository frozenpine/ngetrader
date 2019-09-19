# coding: utf-8
from collections import namedtuple


class Bar(namedtuple("Bar",
                     ("ts", "open", "high", "low", "close", "volume", "symbol"))):
    def to_dict(self):
        return self._asdict()


class Trade(namedtuple("Trade", ("ts", "price", "volume", "symbol"))):
    def to_dict(self):
        return self._asdict()
