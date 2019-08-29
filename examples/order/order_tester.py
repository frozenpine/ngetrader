# coding: utf-8
import yaml
import csv

from pprint import pprint

try:
    from common.utils import path
    from trade.core import Trader
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

    from common.utils import path
    from trade.core import Trader


class OrderTester(Trader):
    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        self._request_orders = dict()

        super(OrderTester, self).__init__(host=host, symbol=symbol,
                                          api_key=api_key,
                                          api_secret=api_secret)

    def on_rtn_order(self, order):
        order_id = order["orderID"]
        if order_id not in self._request_orders:
            return

        print("委托回报:")
        pprint(order)

        if order["ordStatus"] in ("PartiallyFilledCanceled",
                                  "Canceled", "Rejected"):
            self._request_orders.pop(order["orderID"], None)

        if not self._request_orders:
            self.running = False

    def on_rtn_trade(self, trade):
        order_id = trade["orderID"]
        if order_id not in self._request_orders:
            return

        print("成交回报:")
        pprint(trade)

        if trade["ordStatus"] in ("Filled", "PartiallyFilledCanceled"):
            self._request_orders.pop(trade["orderID"], None)

        if not self._request_orders:
            self.running = False

    def make_new_order(self, **kwargs):
        order, rsp = self.Order.Order_new(**kwargs).result()

        if rsp.status_code > 300:
            logging.error("Order failed: {}".format(order))

            return

        self._request_orders[order["orderID"]] = order


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    with open(path("./order_config.yml"), mode='rb') as f:
        config = yaml.safe_load(f)

    ex = OrderTester(**config)

    with open(path("./order_list.csv"), mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            ex.make_new_order(**row)

    ex.join()
