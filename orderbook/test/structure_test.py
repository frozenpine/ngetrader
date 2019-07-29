# coding: utf-8
import unittest
import time

from datetime import datetime

from ..structure import Order
from ..const import OrderType, TimeCondition, Direction


class OrderTest(unittest.TestCase):
    def test_create(self):
        self.assertRaises((TypeError, AttributeError), Order, foo="bar")

        order = Order(orderID="abc123",
                      timeInForce="GoodTillCancel",
                      ordType="MarketIfTouched",
                      timestamp=time.time() * 1000,
                      transactTime="2019-05-24T17:07:16.123Z")

        self.assertEqual(TimeCondition.GoodTillCancel, order["timeInForce"])
        self.assertEqual(OrderType.MarketIfTouched, order["ordType"])
        self.assertIsInstance(order.timestamp, datetime)
        self.assertIsInstance(order.transactTime, datetime)

        # required column missing
        self.assertRaises(AttributeError, Order)

    def test_side(self):
        order1 = Order(orderID="foo", orderQty=10)

        self.assertEqual(Direction.Buy, order1.side)
        self.assertEqual(10, order1.orderQty)

        order2 = Order(orderID="foo", orderQty=-10)

        self.assertEqual(Direction.Sell, order2.side)
        self.assertEqual(10, order2.orderQty)

        with self.assertRaisesRegex(ValueError, "mis-match with order side"):
            Order(orderID="foo", orderQty=10, side="Sell")
