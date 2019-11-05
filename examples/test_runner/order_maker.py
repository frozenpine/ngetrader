# coding: utf-8
import os
import yaml
import csv

from random import choice

try:
    from common.utils import (path, time_ms, TColor,
                              try_parse_num, try_parse_regex)
    from common.data_source import CSVData
except ImportError:
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

    from common.utils import (path, time_ms, TColor,
                              try_parse_num, try_parse_regex)
    from common.data_source import CSVData


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO)

    config_base = path("./config")
    csv_base = path("./csv")

    order_file = os.path.join(csv_base, "order_test.csv")

    with open(os.path.join(config_base, "order_config.yml"), mode='rb') as f:
        config = yaml.safe_load(f)

    test_config = config.get('test')

    if not test_config:
        exit()

    price_list = list(map(
        lambda x: float(test_config.get("base_price", 10000)) + 0.5 * x,
        range(1, int(test_config.get("levels", 50)) + 1, 1)
    ))
    volume_list = [1, 3, 5, 10, 15, 30, 50, 100]
    directions = ("Buy", "Sell")

    with open(order_file, mode='w+', encoding='utf-8', newline='') as f:
        order_writer = csv.DictWriter(
            f, fieldnames=("symbol", "side", "price", "orderQty"))
        order_writer.writeheader()
        order_writer.writerows([{
            'symbol': 'XBTUSD',
            'side': choice(directions),
            'price': choice(price_list),
            'orderQty': choice(volume_list)
        } for _ in range(test_config.get('total', 10000))])
