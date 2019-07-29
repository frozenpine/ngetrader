# coding: utf-8
import unittest
import sys

from ..core import PriceLevel, MBL, OrderBook, PriceHeap
from ..const import Direction
from ..structure import Order


class PriceLevelTest(unittest.TestCase):
    _SYMBOL = "XBTUSD"
    _TICK_PRICE = 0.5
    _LEVEL_PRICE = 256.0

    def setUp(self) -> None:
        self.ob = OrderBook(symbol=self._SYMBOL, tick_price=self._TICK_PRICE)
        self.mbl = MBL(direction=Direction.Buy, orderbook=self.ob)
        self.level = PriceLevel(price=self._LEVEL_PRICE, mbl=self.mbl)

    def test_getitem(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE)

        with self.assertRaisesRegex(IndexError,
                                    r"index\[.*\] out of range"):
            _ = self.level[0]

        self.level.push_order(order1)
        self.level.push_order(order2)

        with self.assertRaises(ValueError):
            _ = self.level["foo"]

        self.assertEqual(order1, self.level[0])
        self.assertEqual(order2, self.level[1])

        self.level.remove_order_by_id("123")
        self.assertEqual(order2, self.level[0])

    def test_append(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE+1)

        self.assertEqual(0, self.level.push_order(order1))

        self.assertEqual(1, self.level.count)

        self.assertEqual(self._LEVEL_PRICE, self.level.level_price)

        # order price mis-match with level price
        with self.assertRaisesRegex(ValueError,
                                    r"order\[.*\]'s price\[.*\] mis-match "
                                    r"with current level\[.*\]"):
            self.level.push_order(order2)

        # order already exist in current level
        with self.assertRaisesRegex(ValueError,
                                    r"order\[.*\] exists in "
                                    r"current level\[.*\]"):
            self.level.push_order(order1)

    def test_modify(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="123", price=self._LEVEL_PRICE)
        order3 = Order(orderID="456", price=self._LEVEL_PRICE)

        # order level is empty
        self.assertEqual(-1, self.level.modify_order(order1))

        self.level.push_order(order1)

        idx = self.level.modify_order(order2)
        self.assertEqual(order2, self.level[idx])

        # order not exist under current level
        with self.assertRaisesRegex(ValueError,
                                    r"order\[.*\] not exists under current "
                                    r"level\[.*\]"):
            self.level.modify_order(order3)

    def test_pop(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE)
        order3 = Order(orderID="789", price=self._LEVEL_PRICE)
        order4 = Order(orderID="foo", price=self._LEVEL_PRICE)

        with self.assertRaisesRegex(ValueError,
                                    r"order\[.*\] not exists in current "
                                    r"level\[.*\]"):
            self.level.remove_order(order1)

        self.level.push_order(order1)

        with self.assertRaisesRegex(ValueError,
                                    r"order\[.*\] not exists in current "
                                    r"level\[.*\]"):
            self.level.remove_order(order2)

        self.level.push_order(order2)
        self.level.push_order(order3)
        self.level.push_order(order4)

        self.assertEqual(order1, self.level.remove_order(order1))

        self.assertEqual(order2, self.level[0])

        self.assertEqual(order3, self.level.remove_order_by_id("789"))
        self.assertEqual(order2, self.level[0])
        self.assertEqual(order4, self.level[1])

    def test_trade(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE, orderQty=1)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE, orderQty=2)
        order3 = Order(orderID="foo", price=self._LEVEL_PRICE, orderQty=3)
        order4 = Order(orderID="bar", price=self._LEVEL_PRICE, orderQty=4)

        self.level.push_order(order1)
        self.level.push_order(order2)
        self.level.push_order(order3)
        self.level.push_order(order4)

        self.assertTrue(self._LEVEL_PRICE in self.mbl)

        remained, orders = self.level.trade_volume(3)
        self.assertEqual(0, remained)
        self.assertEqual([order1, order2], [r() for r in orders])

        remained, orders = self.level.trade_volume(4)
        self.assertEqual(0, remained)
        self.assertEqual([order3, order4], [ref() for ref in orders])
        self.assertEqual(order4, self.level[0])
        self.assertEqual(3, self.level[0]["leavesQty"])

        remained, orders = self.level.trade_volume(5)
        self.assertEqual(2, remained)
        self.assertEqual([order4], [ref() for ref in orders])

        self.assertFalse(self._LEVEL_PRICE in self.mbl)


class PriceHeapTest(unittest.TestCase):
    def test_len(self):
        heap = PriceHeap(direction=Direction.Buy)

        self.assertEqual(0, len(heap))

        for price in range(100):
            heap.push(price)

        self.assertEqual(100, len(heap))

    def test_bool(self):
        heap = PriceHeap(direction=Direction.Sell)

        self.assertTrue(not heap)

        heap.push(1)

        self.assertFalse(not heap)

    def test_sort(self):
        buy = PriceHeap(direction=Direction.Buy)

        sell = PriceHeap(direction=Direction.Sell)

        for price in range(1, 101, 1):
            buy.push(price)
            sell.push(price)

        self.assertEqual(1, sell[0])
        self.assertEqual(100, buy[0])

        self.assertEqual([1, 2, 3, 4, 5], sell.top(5))
        self.assertEqual([100, 99, 98, 97, 96], buy.top(5))

        self.assertEqual(100, sell.worst_price)
        self.assertEqual(1, buy.worst_price)

        # 99 price left
        self.assertEqual(1, sell.pop())
        self.assertEqual(100, buy.pop())

        self.assertEqual(2, sell[0])
        self.assertEqual(99, buy[0])

        # 49 price w/ price 1 already missing
        for price in range(1, 50, 1):
            sell.remove(price)

        self.assertEqual(51, len(sell))
        self.assertEqual(50, sell[0])
        self.assertEqual([50, 51, 52], sell.top(3))

        # 51 price w/ price 100 already missing
        for price in range(50, 101, 1):
            buy.remove(price)

        self.assertEqual(49, len(buy))
        self.assertEqual(49, buy[0])
        self.assertEqual([49, 48, 47], buy.top(3))


class MBLTest(unittest.TestCase):
    _SYMBOL = "XBTUSD"
    _TICK_PRICE = 0.5

    def setUp(self) -> None:
        self.ob = OrderBook(symbol=self._SYMBOL, tick_price=self._TICK_PRICE)
        self.sell = MBL(direction=Direction.Sell, orderbook=self.ob)
        self.buy = MBL(direction=Direction.Buy, orderbook=self.ob)

    def test_create(self):
        self.assertEqual(sys.float_info.max, self.sell.best_price)
        self.assertEqual(0, self.buy.best_price)

        for i in range(1, 6, 1):
            order = Order(orderID=str(i), price=i)

            self.buy.add_order(order)

        self.assertEqual(5, self.buy.best_price)
        self.assertEqual(5, self.buy.best_level.level_price)
        self.assertEqual(5, self.buy.depth)

        for i in range(1, 6, 1):
            order = Order(orderID=str(i), price=i, side="Sell")

            self.sell.add_order(order)

        self.assertEqual(1, self.sell.best_price)
        self.assertEqual(1, self.sell.best_level.level_price)

        self.assertEqual(5, self.sell.depth)

    def test_append_level(self):
        level1 = PriceLevel(price=0.0, mbl=self.buy)

        with self.assertRaisesRegex(ValueError, r"invalid level price\[.*\]"):
            self.buy.append_level(level1)

        # if price valid and mbl specified,
        # price level will add to mbl automatically
        level2 = PriceLevel(price=1.0, mbl=self.buy)

        self.assertEqual(1, self.buy.depth)

        with self.assertRaisesRegex(ValueError,
                                    r"level with price\[.*\] already exists."):
            self.buy.append_level(level2)

        level3 = PriceLevel(price=2.0)

        self.buy.append_level(level3)
        self.assertEqual(2, self.buy.depth)
        self.assertEqual(level3.mbl, self.buy)

        with self.assertRaisesRegex(RuntimeError,
                                    "mbl setter can only be called by "
                                    "MBL.append_level"):
            level3.mbl = self.buy

        with self.assertRaisesRegex(ValueError,
                                    "level is already append to another mbl."):
            self.sell.append_level(level2)
