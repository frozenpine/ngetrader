# coding: utf-8
import logging

from pprint import pprint

from trade.core import Trader


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # from os import environ
    # environ["http_proxy"] = "http://127.0.0.1:1080"
    # environ["https_proxy"] = "http://127.0.0.1:7890"
    # bitmex = Trader(host="https://www.bitmex.com")
    # bitmex.kline.retrieve_bars(endpoint="/api/udf/history",
    #                            mode="last", trigger=True)

    # ybmex = Trader(host="http://47.103.74.144",
    #                api_key="1B63T5cP9xjc4QVxuwDD",
    #                api_secret="52QT8279xmFXtKK0X2SMKkcf07yv6e2zb79PGAhy1Hu8Fh"
    #                           "1FiQgnK6G1Q2c6aEug936JH536yyLaLAz9b3HcNvj4GYTD5sM02xr")
    ybmex = Trader(host="http://47.103.74.144")

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
