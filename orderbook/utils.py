# coding: utf-8
from decimal import Decimal, Context
from datetime import datetime

from orderbook import logger


# noinspection SpellCheckingInspection
def normalize_price(price: float, tick_price: float) -> float:
    decimal_origin = Decimal(price)

    decimal_tick = Decimal(tick_price)

    # 获取 price 整数位数
    int_len = len("{:f}".format(price).split(".")[0])

    # 获取 tick_price 小数位数
    # 超过6位小数，float -> str 将使用科学计数法
    prec_str = str(tick_price).rstrip("0")
    if "." in prec_str:
        prec_len = len(prec_str.split(".")[-1])
    else:
        prec_len = abs(int(prec_str.split("e")[-1]))

    ticks = round(decimal_origin / decimal_tick)

    normalized = ticks * Decimal(tick_price)

    converted = float(normalized.normalize(Context(prec=int_len+prec_len)))

    logger.debug(
        "normalize price: origin[{}], tick_price[{}], normalized[{}].".format(
            price, tick_price, converted))

    return converted


def make_datetime(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value / 1000)

    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")

    raise ValueError("invalid timestamp: {}".format(value))
