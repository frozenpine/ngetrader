# coding: utf-8

import unittest

from ..const import (new_direction, Direction, OrderStatus,
                     create_enum_by_name)


class DirectionTest(unittest.TestCase):
    def test_new(self):
        buy = new_direction(1)
        sell = new_direction("Sell")

        self.assertEqual(buy, Direction.Buy)
        self.assertEqual(sell, Direction.Sell)

        self.assertRaises(ValueError, new_direction, 3)
        self.assertRaises(ValueError, new_direction, "test")

        self.assertNotEqual(new_direction(-1), new_direction("buy"))

    def test_print(self):
        self.assertEqual(str(Direction.Buy), "Buy")
        self.assertEqual(str(Direction.Sell), "Sell")

        print(Direction.Buy, Direction.Sell)

    def test_flap(self):
        self.assertEqual(Direction.Buy.flap(), Direction.Sell)

    def test_value(self):
        self.assertEqual(Direction.Buy.value, 1)
        self.assertEqual(Direction.Sell.value, -1)


class OrderStatusTest(unittest.TestCase):
    def test_finish(self):
        for status in (OrderStatus.Rejected, OrderStatus.Filled,
                       OrderStatus.PartiallyFilledCanceled,
                       OrderStatus.Canceled):
            self.assertTrue(status.is_finished())

        for status in (OrderStatus.New, OrderStatus.PartiallyFilled):
            self.assertFalse(status.is_finished())

    def test_migrate(self):
        order_status = (OrderStatus.Canceled, OrderStatus.New,
                        OrderStatus.PartiallyFilled,
                        OrderStatus.PartiallyFilledCanceled,
                        OrderStatus.Filled, OrderStatus.Rejected)

        for status in (OrderStatus.Rejected, OrderStatus.Filled,
                       OrderStatus.PartiallyFilledCanceled,
                       OrderStatus.Canceled):
            for test_status in order_status:
                self.assertIsNone(status.migrate(test_status))

        for status in (OrderStatus.Canceled, OrderStatus.PartiallyFilled,
                       OrderStatus.Filled, OrderStatus.Rejected):
            next_status = OrderStatus.New.migrate(status)

            self.assertIsNotNone(next_status)
            self.assertEqual(status, next_status)

        for status in (OrderStatus.PartiallyFilledCanceled,
                       OrderStatus.PartiallyFilled):
            next_status = OrderStatus.PartiallyFilled.migrate(status)

            self.assertIsNotNone(next_status)
            self.assertEqual(status, next_status)

    def test_create(self):
        order_status = create_enum_by_name(OrderStatus, "Canceled")

        self.assertEqual(order_status, OrderStatus.Canceled)

        self.assertRaises(ValueError, create_enum_by_name,
                          OrderStatus, "test")
