# coding: utf-8
import time
import statistics

from collections import defaultdict
from random import shuffle

try:
    from orderbook.core import OrderBook, PriceLevel
    from orderbook.structure import Order
except ImportError:
    import sys
    import os

    CURRENT_DIR = os.path.dirname(sys.argv[0])

    sys.path.append(os.path.join(CURRENT_DIR, "../"))

    from orderbook.core import OrderBook, PriceLevel
    from orderbook.structure import Order


if __name__ == "__main__":
    order_price = 100.0

    order_count = 10000

    metrics = defaultdict(list)

    for idx in range(100):
        level = PriceLevel(price=order_price)

        orders_volumes = list(range(1, order_count + 1, 1))

        add_order_start = time.time()
        for vol in orders_volumes:
            level.push_order(
                Order(orderID=str(vol), price=order_price,
                      orderQty=vol)
            )
        order_time_span = time.time() - add_order_start

        metrics["order"].append(order_count / order_time_span)

        print("{:d}# PriceLevel order input rate: {:.2f} ops".format(
            idx + 1, metrics["order"][-1]))

        shuffle(orders_volumes)

        split = int(order_count * 0.8)

        cancel_volumes = orders_volumes[:split]
        trade_volumes = orders_volumes[split:]

        cancel_start = time.time()
        for order_idx in cancel_volumes:
            level.remove_order_by_id(str(order_idx))
        cancel_time_span = time.time() - cancel_start

        metrics["cancel"].append(len(cancel_volumes) / cancel_time_span)
        print("{:d}# PriceLevel order cancel rate: {:.2f} ops".format(
            idx + 1, metrics["cancel"][-1]))

        trade_count = 0
        trade_vol_start = time.time()
        for vol in trade_volumes:
            remained, traded = level.trade_volume(vol)
            trade_count += len(traded)
        trade_time_span = time.time() - trade_vol_start

        metrics["trade"].append(trade_count / trade_time_span)

        print("{:d}# PriceLevel order trade rate: {:.2f} ops".format(
            idx + 1, metrics["trade"][-1]))

        print()

        del level

    for key, value_list in metrics.items():
        max_rate = max(value_list)
        min_rate = min(value_list)
        mean_rate = statistics.mean(value_list)
        stdev_rate = statistics.stdev(value_list)

        print(
            "{} rate metrics: Max[{:.2f}], Min[{:.2f}], "
            "Avg[{:.2f}], Std[{:.2f}@{:.2f} %]".format(
                key, max_rate, min_rate, mean_rate, stdev_rate,
                stdev_rate / mean_rate * 100)
        )
