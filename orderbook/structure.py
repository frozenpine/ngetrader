# coding: utf-8
__all__ = ("Order", "Trade")

from datetime import datetime

from orderbook.utils import make_datetime
from orderbook.const import (create_enum_by_name, Direction, OrderStatus,
                             OrderType, TimeCondition)


class DataModel(object):
    __slots__ = dict()

    _column_mapper_ = dict()
    _required_columns_ = set()

    def __init__(self, **kwargs):
        missing_columns = self._required_columns_ - kwargs.keys()
        if missing_columns:
            raise AttributeError("columns[{}] is required.".format(
                ",".join(missing_columns)))

        for key, value in kwargs.items():
            if key in self._column_mapper_:
                value = self._column_mapper_[key](value)

            setattr(self, key, value)

    def __getattr__(self, item):
        if item not in self.__slots__:
            raise AttributeError("invalid attribute name: {}".format(item))

        return self.__slots__[item]

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError("invalid attribute name: {}".format(item))

    def __setitem__(self, key, value):
        try:
            return setattr(self, key, value)
        except AttributeError:
            raise KeyError("invalid attribute name: {}".format(key))


class Order(DataModel):
    __slots__ = {"orderID": "", "clOrdID": "", "clOrdLinkID": "",
                 "account": "", "symbol": "", "side": Direction.Buy,
                 "simpleOrderQty": 0.0, "orderQty": 0, "price": 0.0,
                 "displayQty": 0, "stopPx": 0.0, "pegOffsetValue": 0.0,
                 "pegPriceType": "", "currency": "", "settleCurrency": "",
                 "ordType": OrderType.Limit,
                 "timeInForce": TimeCondition.GoodTillCancel, "execInst": "",
                 "contingencyType": "", "exDestination": "",
                 "ordStatus": OrderStatus.New, "triggered": "",
                 "workingIndicator": False, "ordRejReason": "",
                 "simpleLeavesQty": 0.0, "leavesQty": 0, "simpleCumQty": 0.0,
                 "cumQty": 0, "avgPx": 0.0, "multiLegReportingType": "",
                 "text": "", "transactTime": None, "timestamp": None,
                 "__weakref__": None}

    _required_columns_ = {"orderID"}

    _column_mapper_ = {
        "side": lambda v: create_enum_by_name(Direction, v),
        "ordStatus": lambda v: create_enum_by_name(OrderStatus, v),
        "ordType": lambda v: create_enum_by_name(OrderType, v),
        "timeInForce": lambda v: create_enum_by_name(TimeCondition, v),
        "timestamp": make_datetime,
        "transactTime": make_datetime
    }

    def __init__(self, **kwargs):
        super(Order, self).__init__(**kwargs)

        if "orderQty" in kwargs:
            qty = kwargs["orderQty"]

            if "side" in kwargs:
                if (qty > 0) ^ (self["side"].value > 0):
                    raise ValueError(
                        "order quantity[{}] mis-match with order side[{}]"
                        .format(qty, kwargs["side"]))
            else:
                setattr(self, "side",
                        Direction(qty / abs(qty)))

            setattr(self, "orderQty", abs(qty))

        if "leavesQty" not in kwargs:
            setattr(self, "leavesQty", self["orderQty"])

    def __eq__(self, other):
        """
        Compare two order, if orderID and timestamp is same,
        two order is equal.
        :param other:
        :return:
        """
        return (self["orderID"] == other["orderID"] and
                self["timestamp"] == other["timestamp"])

    def __lt__(self, other):
        """
        Compare one order to another
        if order id is same, then timestamp is compare condition
        if order is is different, order's worth(price * leavesQty) is compare
        condition
        :param other: another order
        :return: bool
        """
        if hash(self) != hash(other):
            return (self["price"] * self["leavesQty"] <
                    other["price"] * other["leavesQty"])

        return self["timestamp"] < other["timestamp"]

    def __hash__(self):
        """
        Order id used as hash object
        :return:
        """
        return hash(self["orderID"])


class Trade(DataModel):
    __slots__ = {"timestamp": datetime.now(), "symbol": "",
                 "side": None, "size": 0, "price": 0.0,
                 "tickDirection": None, "trdMatchID": "", "grossValue": 0,
                 "homeNotional": 0.0, "foreignNotional": 0.0,
                 "__weakref__": None}

    _column_mapper_ = {
        "timestamp": make_datetime,
        "tickDirection": lambda v: create_enum_by_name(Direction, v),
        "side": lambda v: create_enum_by_name(Direction, v)
    }

    _required_columns_ = {"symbol", "timestamp"}
