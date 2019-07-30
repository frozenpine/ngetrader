# coding: utf-8
import logging
import arrow

# noinspection PyUnresolvedReferences
from os import environ
from time import time, sleep
from datetime import datetime
from collections import defaultdict, Counter
from threading import Thread

from clients.nge_websocket import NGEWebsocket

logging.basicConfig(level=logging.DEBUG)


def convert_timestamp(value):
    if isinstance(value, int):
        return value

    ts = arrow.get(value)

    return ts.float_timestamp * 1000


class MarketTicker(NGEWebsocket):
    metrics = defaultdict(Counter)

    link_latency = 0
    timestamp_fix = 0

    def insert_handler(self, table_name, message):
        receive_timestamp = time() * 1000

        super(MarketTicker, self).insert_handler(table_name, message)

        if table_name != "trade":
            return

        self.metrics["trade"]["total"] += len(message["data"])

        for trade in message["data"]:
            trade_timestamp = convert_timestamp(trade["timestamp"])

            timestamp_lag = receive_timestamp - trade_timestamp

            self.timestamp_fix = min(timestamp_lag, self.timestamp_fix)

            timestamp_lag = (timestamp_lag - self.timestamp_fix -
                             self.link_latency)

            if timestamp_lag >= 1000:
                self.metrics["trade"]["late_total"] += 1

                print("Trade[{}] received at {} "
                      "late for {} ms(fixed by {} ms): {}".format(
                        datetime.fromtimestamp(trade_timestamp / 1000),
                        datetime.fromtimestamp(receive_timestamp / 1000),
                        timestamp_lag, abs(self.timestamp_fix), trade))


def late_summary(ticker: MarketTicker):
    while ticker.wst.is_alive():
        if not ticker.link_latency:
            sleep(1)
            continue

        temp_metrics = ticker.metrics.copy()

        for table, counter in temp_metrics.items():
            print("{}'s metrics: total[{}], "
                  "late[{}], {:.2f} %, link[{} ms]".format(
                    table, counter["total"], counter["late_total"],
                    counter["late_total"] / counter["total"] * 100,
                    ticker.link_latency))

        del temp_metrics

        sleep(5)


def link_latency_test(ticker: MarketTicker):
    import socks
    import socket
    import urllib

    # noinspection PyUnresolvedReferences
    remote_addr = urllib.parse.urlparse(ticker.endpoint).netloc.split(":")

    if "https_proxy" in environ:
        socks5 = environ["https_proxy"].lstrip("htp:/").split(":")
        socks.set_default_proxy(socks.PROXY_TYPE_SOCKS5,
                                socks5[0], int(socks5[1]))
        socket.socket = socks.socksocket

    if len(remote_addr) < 2:
        remote_addr.append(80)

    while ticker.wst.is_alive():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)

            start = time()
            sock.connect(tuple(remote_addr))
        except Exception as e:
            ticker.logger.error(e)
        else:
            time_span = time() - start
            ticker.link_latency = int(round(time_span * 1000 / 3))

            sock.close()
        finally:
            sleep(60)


if __name__ == "__main__":
    # environ["https_proxy"] = "http://127.0.0.1:7890"
    tk = MarketTicker(host="https://www.ybmex.com",
                      symbol="XBTUSD")

    metric_tr = Thread(target=late_summary, args=(tk,))
    metric_tr.daemon = True
    metric_tr.start()

    link_tr = Thread(target=link_latency_test, args=(tk,))
    link_tr.daemon = True
    link_tr.start()

    tk.wst.join()
