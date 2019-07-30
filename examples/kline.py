# coding: utf-8
import logging
import sys

from pprint import pprint

from trade.core import Trader


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from os import environ
    environ["http_proxy"] = "http://127.0.0.1:1080"
    # environ["https_proxy"] = "http://127.0.0.1:7890"
    # bitmex = Trader(host="https://www.bitmex.com")
    # bitmex.kline.retrieve_bars(endpoint="/api/udf/history",
    #                            mode="last", trigger=True)

    ybmex = Trader()
    # ybmex.kline.retrieve_bars(
    #     precise="5", count=100,
    #     to_ts=arrow.now().shift(minutes=-100), trigger=True)
    # ybmex.kline.retrieve_bars(
    #     precise="5", count=100,
    #     to_ts=arrow.now().shift(minutes=-100))
    #
    # print("total {} candle retrieved.".format(len(ybmex.kline)))
    # pprint(ybmex.kline.latest_bar_data)

    ybmex.kline.retrieve_bars()

    for bar in ybmex.kline:
        pprint(bar)

    ybmex.join()
