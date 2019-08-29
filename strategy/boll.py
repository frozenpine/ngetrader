# coding: utf-8
import talib
import numpy as np

from collections import namedtuple
from pprint import pprint

from trade.kline import Bar
from strategy import BaseStrategy


class Boll:
    BOLL_WIDTH = 20
    BOLL_DEV = 2

    BAND_TICK = namedtuple("BollBandTick", ("up", "middle", "down"))

    def __init__(self):
        self._boll_up = np.array(list())
        self._boll_middle = np.array(list())
        self._boll_down = np.array(list())

    # noinspection PyUnresolvedReferences
    def band(self, last_price):
        close_price = getattr(self, "_close_price")

        self._boll_up, self._boll_middle, self._boll_down = talib.BBANDS(
            np.append(close_price, last_price), timeperiod=self.BOLL_WIDTH,
            nbdevup=self.BOLL_DEV, nbdevdn=self.BOLL_DEV,
            matype=talib.MA_Type.SMA
        )

        return self.BAND_TICK(self._boll_up[-1],
                              self._boll_middle[-1],
                              self._boll_down[-1])


class BollTrend(BaseStrategy, Boll):
    BOLL_DEV = 1

    WMA_WIDTH = 10

    ROC_WIDTH = 10
    ROC_BURST = 1

    VOLUME = 1000

    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret="",
                 kline_opts=None):
        super(BollTrend, self).__init__(host=host, symbol=symbol,
                                        api_key=api_key, api_secret=api_secret,
                                        kline_opts=kline_opts)
        Boll.__init__(self)

    def __open_signal(self, last_price: np.float64,
                      boll_band: Boll.BAND_TICK,
                      wma: np.float64, roc: np.float64):
        if self._position or roc == 0:
            return ""

        side = "Buy" if roc > 0 else "Sell"
        bar_open_price = (self.kline.latest_bar_data["open"] if
                          self.kline.latest_bar_data["open"] else last_price)

        if roc > 0:
            if bar_open_price > boll_band.up:
                return ""

            if wma <= boll_band.middle:
                return ""

            if roc >= self.ROC_BURST:
                self.logger.info("big trends occurred open.")
                return side

            if last_price >= boll_band.up:
                self.logger.info("price breakthrough up limit open.")
                return side

            return ""

        if roc < 0:
            if bar_open_price < boll_band.down:
                return ""

            if wma >= boll_band.middle:
                return ""

            if roc <= -self.ROC_BURST:
                self.logger.info("big trends occurred open.")
                return side

            if last_price <= boll_band.down:
                self.logger.info("price breakthrough down limit open.")
                return side

            return ""

        return ""

    def __close_signal(self, last_price: np.float64,
                       boll_band: Boll.BAND_TICK,
                       wma: np.float64, roc: np.float64):
        if not self._position or roc == 0:
            return ""

        side = "Buy" if self._position["side"] == "Sell" else "Sell"

        if side == "Buy":
            if wma >= boll_band.middle or roc > self.ROC_BURST:
                self.logger.info("big trends occurred close.")
                return side

            if last_price > boll_band.down:
                self.logger.info("price breakthrough down limit close.")
                return side

            return ""

        if side == "Sell":
            if wma <= boll_band.middle or roc < -self.ROC_BURST:
                self.logger.info("big trends occurred close.")
                return side

            if last_price < boll_band.up:
                self.logger.info("price breakthrough up limit close.")
                return side

            return ""

        return ""

    # noinspection PyUnresolvedReferences
    def __judge_action(self, trade_data: dict):
        last_price = trade_data["price"]

        last_boll = self.band(last_price)
        close_price_array = np.append(self._close_price, last_price)
        last_wma = talib.WMA(close_price_array, timeperiod=self.WMA_WIDTH)[-1]
        last_roc = talib.ROC(close_price_array, timeperiod=self.ROC_WIDTH)[-1]

        print("LST[{:.1f}] BUP[{:.4f}] BMD[{:.4f}] BDN[{:.4f}] "
              "WMA[{:.4f}] ROC[{:.4f}]".format(
                last_price, last_boll.up, last_boll.middle,
                last_boll.down, last_wma, last_roc))

        open_side = self.__open_signal(last_price=last_price,
                                       boll_band=last_boll,
                                       wma=last_wma,
                                       roc=last_roc)
        if open_side:
            open_price = (self.best_buy["price"] if
                          open_side == "Sell" else
                          self.best_sell["price"])
            open_size = self.VOLUME if open_side == "Buy" else -self.VOLUME
            self._position = {"side": open_side,
                              "price": open_price,
                              "size": open_size}

            print("{} open {}@{:.1f}.".format(open_side, open_size, open_price))

            return

        close_side = self.__close_signal(last_price=last_price,
                                         boll_band=last_boll,
                                         wma=last_wma,
                                         roc=last_roc)
        if close_side:
            close_price = (self.best_buy["price"] if
                           close_side == "Sell" else
                           self.best_sell["price"])
            close_size = self._position["size"]

            profit = ((close_price - self._position["price"]) * close_size)

            self._profit.append(profit)

            self._position = None

            print("{} close {}@{:.1f} with profit {}.".format(
                close_side, close_size, close_price, profit))

            return

    def on_bar(self, bar: Bar):
        super(BollTrend, self).on_bar(bar)

        if len(self._close_price) > self.BOLL_WIDTH * 3:
            self._close_price = self._close_price[
                -int(len(self._close_price)/2):]

        pprint(bar)

    def on_trade(self, trade_data: dict):
        if len(self.kline) < self.BOLL_WIDTH:
            return

        self.__judge_action(trade_data)


if __name__ == "__main__":
    import logging
    from os import environ

    environ["https_proxy"] = "http://127.0.0.1:7890"

    logging.basicConfig(level=logging.INFO)

    kline_opts = {
        "endpoint": "/api/udf/history",
        "mode": "last"
    }

    td = BollTrend(host="https://www.bitmex.com", kline_opts=kline_opts)
    # td = BollTrend()

    td.join()
