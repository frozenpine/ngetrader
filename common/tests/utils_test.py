# coding: utf-8
import unittest

class TestTColor(unittest.TestCase):
    from common.utils import TColor

    def test_fg_color(self):
        self.TColor.list_colors()

        print(self.TColor.highlight("red", "red"))

    def test_regex(self):
        print(self.TColor.regex_rend(r"aaa","lkajskdfaaalkjsdfh", fg="red",
                                     bg="white"))

        print(self.TColor.regex_highlight(r'[\'"]orderStatus[\'"]: ?[^,}]+',
                                          str({"orderStatus": "New"}),
                                          "red"))