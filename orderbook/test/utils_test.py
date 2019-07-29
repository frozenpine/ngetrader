# coding: utf-8

import unittest

from ..utils import normalize_price


class UtilsTests(unittest.TestCase):
    def test_normalize_price(self):
        self.assertEqual(normalize_price(15.486, 0.01), 15.49)

        self.assertEqual(
            normalize_price(15.486765123653, 0.00000000001),
            15.48676512365)
