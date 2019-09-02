# coding: utf-8
import unittest

from ..runner import RequestCache


class TestRequestCache(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = RequestCache()

    def test_inflight(self):
        self.cache["a"] = 1

        self.assertTrue(self.cache.is_inflight("a"))

        self.cache["a"] = 2

        self.assertFalse(self.cache.is_inflight("a"))

        self.assertEqual(self.cache["a"], 2)
        self.assertEqual(self.cache.get("a"), 2)

        del self.cache["a"]

        self.assertFalse("a" in self.cache)

    def test_iter(self):
        data = (("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5))

        for k, v in data:
            self.cache[k] = v

        for idx, (k, v) in enumerate(self.cache.items()):
            self.assertEqual(data[idx][0], k)
            self.assertEqual(data[idx][1], v)

            self.assertTrue(self.cache.is_inflight(k))

            self.cache[k] = v + 1

            self.assertFalse(self.cache.is_inflight(k))
