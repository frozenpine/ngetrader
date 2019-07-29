# coding: utf-8
__all__ = ("OrderBook", "MBL", "PriceLevel", "PriceHeap")

import heapq
import sys

from os.path import sep

from typing import List, Dict, Optional
from collections import defaultdict, OrderedDict
from functools import reduce, wraps
from weakref import ref, ReferenceType

from orderbook import logger
from orderbook.const import (Direction, create_enum_by_name)
from orderbook.utils import normalize_price
from orderbook.structure import Order


class PriceHeap(object):
    def __init__(self, direction: Direction):
        self._direction = -direction.value

        self._worst_price = None

        self._heap = list()

    @property
    def best_price(self) -> float:
        if self._heap:
            return self._heap[0] * self._direction

        if self._direction > 0:
            return sys.float_info.max
        else:
            return 0.0

    @property
    def worst_price(self) -> float:
        if self._worst_price is not None:
            return self._worst_price * self._direction

        if self._direction > 0:
            return 0.0
        else:
            return sys.float_info.max

    def push(self, price: float):
        price = self._direction * price

        if self._worst_price is None or price > self._worst_price:
            self._worst_price = price

        heapq.heappush(self._heap, price)

    def pop(self) -> float:
        return self._direction * heapq.heappop(self._heap)

    def remove(self, price: float):
        try:
            self._heap.remove(price * self._direction)
        except ValueError as e:
            logger.warning(e)
            return

        heapq.heapify(self._heap)

    def top(self, n: int = 25) -> List[float]:
        n = min(n, len(self._heap))

        return [p * self._direction for p in heapq.nsmallest(n, self._heap)]

    def top_price(self, price: float):
        pass

    def __getitem__(self, item):
        return self._heap[item] * self._direction

    def __len__(self):
        return len(self._heap)

    def __bool__(self):
        if self._heap:
            return True

        return False


class OrderBook(object):
    def __init__(self, symbol: str, tick_price: float, max_depth=-1):
        self._symbol = symbol

        if not isinstance(tick_price, float):
            raise ValueError(
                "invalid tick_price: {}, must be a float.".format(tick_price))

        self._tick_price = tick_price

        self._max_depth = max_depth

        self._mbl = {
            Direction.Sell: MBL(direction=Direction.Sell, orderbook=self),
            Direction.Buy: MBL(direction=Direction.Buy, orderbook=self)
        }

        self._order_price_index_map = dict()

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def tick_price(self) -> float:
        return self._tick_price

    @property
    def max_depth(self) -> int:
        return self._max_depth

    @property
    def buy_mbl(self):
        """
        :rtype MBL
        """
        return self._mbl[Direction.Buy]

    @property
    def sell_mbl(self):
        """
        :rtype MBL
        """
        return self._mbl[Direction.Sell]

    def get_price_direction(self, price) -> Optional[Direction]:
        if price >= self.sell_mbl.best_price:
            return Direction.Sell

        if price <= self.buy_mbl.best_price:
            return Direction.Buy

        return None

    def in_gap(self, price: float) -> bool:
        return self.buy_mbl.best_price < price < self.sell_mbl.best_price

    def overlap_levels(self, price) -> (Optional[Direction], list):
        direction = self.get_price_direction(price)
        overlapped = list()

        while not self.in_gap(price):
            level = self._mbl[direction].pop_level()

            if level:
                overlapped.append(level)

        return direction, overlapped

    def __getitem__(self, item):
        """
        Get mbl or price level by value
        if item is Direction or Direction string, return mbl
        if item is float price, return price level or None(if level not exist)
        :param item: direction instance/str or price
        :return: mbl or price level
        :raise ValueError
        """
        if isinstance(item, Direction):
            return self._mbl[item]

        if isinstance(item, str):
            direction = create_enum_by_name(Direction, item)

            return self._mbl[direction]

        if isinstance(item, float):
            direction = self.get_price_direction(item)

            if direction:
                return self._mbl[direction][item]

        raise ValueError("invalid index key for mbl side: {}".format(item))


class MBL(object):
    def __init__(self, direction: Direction, orderbook: OrderBook):
        self._direction = direction
        self._orderbook = orderbook

        self._price_heap = PriceHeap(direction=direction)

        self._level_cache = defaultdict(
            lambda: PriceLevel(price=0.0, mbl=self))

    def __check_depth(self):
        assert len(self._level_cache) == len(self._price_heap), \
            ("mbl depth miss match in price heap and level cache: \n"
             "price heap: {}\nlevel cache: {}").format(
                self._price_heap, self._level_cache)

        return len(self._price_heap) > 0

    def __judge_worst_price(self, price):
        if not self._price_heap:
            return True

        switch = {
            Direction.Sell: lambda: price > self._price_heap.worst_price,
            Direction.Buy: lambda: price < self._price_heap.worst_price
        }

        return switch[self._direction]()

    def __get_counterparty(self):
        return self._orderbook[self._direction.flap()]

    @property
    def direction(self) -> Direction:
        return self._direction

    @property
    def best_price(self) -> float:
        """
        Get best level's price
        :return: best level price
        :raise RuntimeError
        """

        return self._price_heap.best_price

    @property
    def best_level(self):
        """
        Get best price level in mbl
        Highest price level in Buy side
        Lowest price level in Sell side
        :return: price level
        :rtype Union[PriceLevel, None]
        :raise RuntimeError
        """

        return self._level_cache.get(self.best_price, None)

    @property
    def depth(self) -> int:
        """
        Level depth of mbl
        :return: level count
        :raise RuntimeError
        """
        if self.__check_depth():
            return len(self._price_heap)

        return 0

    def append_level(self, level):
        """
        Append exist price level to current mbl
        :param level: price level
        :return:
        """
        if not level.level_price:
            raise ValueError(
                "invalid level price[{}]".format(level.level_price))

        if level.level_price in self._level_cache:
            raise ValueError("level with price[{}] already exists.".format(
                level.level_price))

        if level.mbl:
            if level.mbl is not self:
                raise ValueError(
                    "level is already append to another mbl.")
        else:
            level.mbl = self

        self._level_cache[level.level_price] = level
        self._price_heap.push(level.level_price)

    def delete_level(self, price):
        """
        Delete a price level by price
        :param price: level price
        :return: price level
        :rtype Optional(PriceLevel)
        """

        if price not in self._level_cache:
            return None

        level = self._level_cache.pop(price)

        self._price_heap.remove(price)

        return level

    def add_order(self, order: Order) -> int:
        if order["side"] != self._direction:
            raise ValueError(
                "order[{}]'s direction[{}] mis-match with current mbl[]"
                .format(order["orderID"], order["side"], self._direction))

        normalized_price = normalize_price(order["price"],
                                           self._orderbook.tick_price)
        order.price = normalized_price

        if normalized_price not in self._level_cache:
            self._price_heap.push(normalized_price)

        return self._level_cache[normalized_price].push_order(order)

    def trade_volume(self, volume: int) -> (int, Dict[float,
                                                      List[ReferenceType]]):
        remained_volume = volume
        traded_levels = dict()

        while remained_volume > 0 and self.__check_depth():
            best_level = self.best_level

            remained_volume, traded_levels[
                best_level.level_price] = best_level.trade_volume(
                remained_volume)

        return remained_volume, traded_levels

    def pop_level(self):
        """
        Pop best price level
        :return: PriceLevel
        :rtype Optional(PriceLevel)
        """
        if self.__check_depth():
            return None

        return self._level_cache.pop(self._price_heap.pop())

    def __contains__(self, price):
        """
        Check if price exists in mbl
        :param price:
        :return: exists
        """
        price = normalize_price(price, self._orderbook.tick_price)

        return price in self._level_cache

    def __getitem__(self, price):
        """
        Get price level by price
        :param price: level price
        :return: price level
        :rtype PriceLevel
        """

        price = normalize_price(price, self._orderbook.tick_price)

        if price in self._level_cache:
            return self._level_cache[price]

        return None


def _price_level_depth_checker(func):
    @wraps(func)
    def depth_checker(self, *args, **kwargs):
        result = func(self, *args, **kwargs)

        if self.count <= 0 and self._mbl:
            self._mbl.delete_level(self.level_price)

        return result

    return depth_checker


class PriceLevel(object):
    def __init__(self, price: float = 0.0, mbl: MBL = None):
        self._price = price
        self._mbl = mbl

        self._order_cache = OrderedDict()

        self.__add_to_mbl()

    @property
    def level_price(self):
        return self._price

    @property
    def count(self):
        return len(self._order_cache)

    @property
    def size(self):
        if not self._order_cache:
            return 0

        return reduce(lambda x, y: x + y,
                      [o.orderQty for o in self._order_cache.values()],
                      0)

    @property
    def mbl(self):
        return self._mbl

    @mbl.setter
    def mbl(self, mbl: MBL):
        caller = getattr(sys, '_getframe')()
        while caller.f_code.co_name != "mbl":
            caller = caller.f_code.f_back

        caller = caller.f_back

        if (caller.f_code.co_name != "append_level" or
                not caller.f_code.co_filename.endswith(
                    sep.join(("orderbook", "core.py")))):
            raise RuntimeError(
                "mbl setter can only be called by MBL.append_level.")

        self._mbl = mbl

    def __add_to_mbl(self):
        if self._price and self._mbl and self._price not in self._mbl:
            self._mbl.append_level(self)

    def __verify_order_price(self, order):
        if not self._price:
            self._price = order["price"]

            self.__add_to_mbl()

            return

        if order["price"] != self._price:
            raise ValueError(
                "order[{}]'s price[{}] mis-match with current level[{}]"
                .format(order["orderID"], order["price"], self._price))

    def push_order(self, order: Order) -> int:
        """
        Append order to current price level
        :param order: Order
        :return: order index in current level
        :raise ValueError
        """

        self.__verify_order_price(order)

        if order["orderID"] in self._order_cache:
            raise ValueError(
                "order[{}] exists in current level[{}]\n"
                "origin order: {}\nnew order: {}".format(
                    order["orderID"], self.level_price,
                    self._order_cache[order["orderID"]], order))

        self._order_cache[order["orderID"]] = order

        return self.count - 1

    def modify_order(self, order: Order):
        """
        Modify order under current price level
        :param order: Order
        :return:
        :raises ValueError, RuntimeError
        """

        if order["orderID"] not in self._order_cache:
            raise ValueError(
                "order[{}] not exists.".format(order["orderID"]))

        self._order_cache[order["orderID"]] = order

    def remove_order(self, order: Order):
        """
        Remove a order from current level
        if order's price mis-match with current level,
        an exception will raise
        :param order: Order
        :return: order index in level
        :raise ValueError
        """

        return self.remove_order_by_id(order["orderID"])

    @_price_level_depth_checker
    def remove_order_by_id(self, order_id: str):
        """
        Remove a order by its orderID
        if order id not exist, (-1, None) will return
        :param order_id: order id
        :return: order index, removed order's weak ref
        """

        if order_id not in self._order_cache:
            raise ValueError(
                "order[{}] not exists in current level[{}]".format(
                    order_id, self.level_price))

        return self._order_cache.pop(order_id)

    @_price_level_depth_checker
    def trade_volume(self, volume: int) -> (int, List[ReferenceType]):
        """
        Trade specified volume size
        :param volume: volume size to be traded
        :return: remained volume size, traded order's weakref list
        """
        remained_volume = volume

        traded_orders = list()

        for order_id in list(self._order_cache.keys()):
            remained_volume -= self._order_cache[order_id]["leavesQty"]

            order = self._order_cache[order_id]
            traded_orders.append(ref(order))

            if remained_volume >= 0:
                self._order_cache.pop(order_id)

            if remained_volume <= 0:
                order["leavesQty"] = abs(remained_volume)
                break
            else:
                order["leavesQty"] = 0

        return max(0, remained_volume), traded_orders

    def __getitem__(self, idx):
        if not isinstance(idx, int):
            raise ValueError("index type must be integer")

        if idx >= self.count:
            raise IndexError("index[{}] out of range".format(idx))

        return self._order_cache[list(self._order_cache.keys())[idx]]
