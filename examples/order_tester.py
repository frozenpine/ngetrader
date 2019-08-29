# coding: utf-8
import yaml

from pprint import pprint

from common.utils import path
from trade.core import Trader


class OrderTester(Trader):
    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        super(OrderTester, self).__init__(host=host, symbol=symbol,
                                          api_key=api_key,
                                          api_secret=api_secret)

        self._request_orders = dict()

    def __handle_final_status(self, data):
        if data["ordStatus"] in ("Filled", "PartiallyFilledCanceled",
                                 "Canceled", "Rejected"):
            self._request_orders.pop(data["orderID"], None)

        if not self._request_orders:
            self.running = False

    def on_rtn_order(self, order):
        order_id = order["orderID"]
        if order_id not in self._request_orders:
            return

        pprint(order)

        self.__handle_final_status(order)

    def on_rtn_trade(self, trade):
        order_id = trade["orderID"]
        if order_id not in self._request_orders:
            return

        pprint(trade)

        self.__handle_final_status(trade)


if __name__ == "__main__":
    config = yaml.safe_load(path("order_config.yml"))

    ex = Trader(**config)
