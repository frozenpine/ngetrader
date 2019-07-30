# coding: utf-8
import logging
import arrow
import re
import requests
import sys
import json
import time

from urllib.parse import urlparse, urlunparse
from collections import namedtuple, OrderedDict
from threading import Condition, RLock, Thread, Event
from queue import Queue

from clients.utils import condition_controller, condition_waiter

logger = logging.getLogger(__name__)


class Bar(namedtuple("Bar",
                     ("ts", "open", "high", "low", "close", "volume"))):
    def to_dict(self):
        return self._asdict()


class Kline(object):
    DEFAULT_PRECISE = "3m"
    MAX_KLINE_LEN = 5000

    VALID_RESOLUTION = {
        "": (60, 5, 1),
        "D": (1,)
    }

    Command = namedtuple("kline_cmd", ("name", "data"))

    CONTINUOUS = False

    BAR_DELAY = 3

    def __init__(self, host, symbol, bar_callback, running):
        self._host = host
        self._symbol = symbol

        self._running_flag = running if running else Event()

        self._kline_cache = list()

        self._precise = self.DEFAULT_PRECISE
        self._precise_duration = 0
        self._precise_unit = ""
        self._precise_multiplier = 0
        self._precise_sys = ""

        self._latest_bar_data = None

        self._finished = True
        self._wait_condition = Condition(RLock())
        self._synchronized = Event()

        self._cmd_input = Queue()
        self._callback_cache = {
            "trade": self.__trade_handler,
            "bar": bar_callback
        }

        self._command_tr = Thread(
            target=self.__command_handler,
            args=(self._running_flag, self._synchronized,
                  self._cmd_input, self._callback_cache))
        self._command_tr.daemon = True
        self._command_tr.start()

        self._notifier_tr = Thread(
            target=self.__bar_notify_trigger,
            args=(self._running_flag, self.notify_trade))
        self._notifier_tr.daemon = True
        self._notifier_tr.start()

    @staticmethod
    def __bar_notify_trigger(running: Event, notify_func):
        running.wait()

        while running.is_set():
            ts = time.time()

            # assume local timestamp has time gap with server
            time.sleep(60 - (ts % 60) + Kline.BAR_DELAY)

            trade_data = {"timestamp": int(round(time.time() * 1000)),
                          "price": 0, "size": 0}
            notify_func(trade_data)

            logger.debug("sending notify trade data: {}".format(trade_data))

    @staticmethod
    def __command_handler(running: Event, synced: Event,
                          cmd_input: Queue, callbacks: dict):
        running.wait()

        while running.is_set():
            synced.wait()

            cmd = cmd_input.get()

            try:
                callback_func = callbacks[cmd.name]
            except KeyError:
                logger.warning(
                    "unknown callback[{}] with data: {}".format(
                        cmd.name, cmd.data))
                continue

            if not callback_func:
                logger.warning(
                    "invalid callback func for {}".format(cmd.name))
                continue

            try:
                callback_func(cmd.data)
            except Exception as e:
                logger.exception("fail to handle {} data: {}\n{}".format(
                    cmd.name, cmd.data, e))

    @property
    def symbol(self):
        return self._symbol

    @property
    @condition_waiter(bool_attr="_finished",
                      condition_attr="_wait_condition")
    def history(self):
        return tuple(self._kline_cache)

    @property
    def is_synced(self):
        return self._synchronized.is_set()

    def join(self, timeout=None):
        self._command_tr.join(timeout)

    @staticmethod
    def __unit_keywords(unit):
        unit_switch = {
            "m": "minute",
            "h": "hour",
            "D": "day",
            "W": "week"
        }

        try:
            kw = unit_switch[unit]
        except KeyError:
            kw = "minute"

        return kw

    # normalize kline resolution with NGE system's support
    def __convert_resolution(self, duration: str, unit: str):
        def downgrade_unit(d, u):
            unit_downgrade = {
                "W": ("D", 7),
                "D": ("h", 24),
                "h": ("", 60),
                "m": ("", 1)
            }

            u, multiplier = unit_downgrade[u]

            d = d * multiplier

            return d, u

        duration_value = int(duration)
        sys_unit = unit

        while sys_unit not in Kline.VALID_RESOLUTION:
            duration_value, sys_unit = downgrade_unit(
                duration_value, sys_unit)

        for resolution in Kline.VALID_RESOLUTION[sys_unit]:
            if duration_value % resolution == 0:
                self._precise_duration = duration_value
                self._precise_unit = unit
                self._precise_multiplier = duration_value / resolution
                self._precise_sys = "{}{}".format(resolution, sys_unit)

                return

    # normalize from_ts & to_ts
    def __time_range(self, from_ts, to_ts, count, mode):
        mode_switch = {
            "first": lambda t, r: t.shift(**{
                self.__unit_keywords(self._precise_unit)+"s": -r
            }) if r != 0 else t,
            "last": lambda t, r: t.shift(**{
                self.__unit_keywords(self._precise_unit)+"s": -r + 1
            }) if r != 1 else t
        }

        to_timestamp = (arrow.now() if not to_ts else to_ts).ceil(
            self.__unit_keywords(self._precise_unit))

        if not from_ts or from_ts >= to_ts:
            from_timestamp = to_timestamp.shift(**{
                self.__unit_keywords(self._precise_unit)+"s":
                    -int(self._precise_duration) * (count + 1)
            })

            if from_ts:
                logger.warning(
                    "invalid time range[{}:{}], "
                    "discard from_ts.".format(from_ts, to_ts))
        else:
            from_timestamp = from_ts
        from_timestamp = from_timestamp.floor(
            self.__unit_keywords(self._precise_unit))

        # extend from_ts & to_ts
        # make sure no time gap with exist kline
        if self._kline_cache and to_timestamp < self._kline_cache[0].ts:
            to_timestamp = self._kline_cache[0].ts
            logger.info(
                "extend to_ts from {} to {} due to fill time gap with"
                "origin kline cache.".format(to_ts, to_timestamp))
        if self._kline_cache and from_timestamp > self._kline_cache[1].ts:
            from_timestamp = self._kline_cache[-1].ts
            logger.info(
                "extend from_ts from {} to {} due to fill time gap with"
                "origin kline cache.".format(from_ts, from_timestamp))

        from_tick_round = getattr(
            from_timestamp,
            self.__unit_keywords(
                self._precise_unit)) % self._precise_multiplier

        from_timestamp = mode_switch[mode](from_timestamp, from_tick_round)

        return from_timestamp, to_timestamp

    def __new_bar_data(self, pre_bar: Bar = None):
        bar_dict = OrderedDict(
            ts=None,
            open=0.0,
            high=0.0,
            low=sys.float_info.max,
            close=0.0,
            volume=0)

        if pre_bar:
            bar_dict["ts"] = pre_bar.ts.shift(**{
                self.__unit_keywords(self._precise_unit) + "s":
                    self._precise_multiplier
            })

            if self.CONTINUOUS:
                bar_dict["open"] = pre_bar.close

        return bar_dict

    @property
    @condition_waiter(bool_attr="_finished",
                      condition_attr="_wait_condition")
    def latest_bar_data(self):
        if not self._latest_bar_data:
            self.retrieve_bars()

        return self._latest_bar_data.copy()

    @condition_controller(bool_attr="_finished",
                          condition_attr="_wait_condition")
    def retrieve_bars(self, precise=DEFAULT_PRECISE, count=100,
                      from_ts: arrow.arrow.Arrow = None,
                      to_ts: arrow.arrow.Arrow = None,
                      endpoint="/history", mode="first", trigger=False):
        """
        Retrieve history kline bars from NGE trading system.
        :param precise: kline resolution string: (1m, 5m, 1h, 1D, 1W, etc.)
        :param count: candle count, max is MAX_TABLE_LEN
        :param from_ts: kline start datetime in Arrow
        :param to_ts: kline end datetime in Arrow
        :param endpoint: kline endpoint
        :param mode: first means first bar start with from timestamp,
        last means first bar end with from timestamp
        :param trigger: whether trigger on_bar callback when kline data
        retrieved
        :return:
        :raise: ValueError
        """

        self._synchronized.clear()

        precise_secs = re.compile(
            r"(?P<duration>\d+)(?P<unit>[mhDW]?)").match(precise).groupdict()

        if not precise_secs:
            raise ValueError("invalid precise: " + precise)

        if self._kline_cache and precise != self._precise:
            logger.warning(
                "kline resolution[{}] mismatch with origin[{}], "
                "discard history kline.".format(precise, self._precise))

            self._kline_cache = list()

        self._precise = precise

        count = min(self.MAX_KLINE_LEN, count)

        # convert user defined resolution to_ts NGE kline resolution
        self.__convert_resolution(duration=precise_secs["duration"],
                                  unit=precise_secs["unit"])

        from_ts, to_ts = self.__time_range(from_ts=from_ts, to_ts=to_ts,
                                           count=count, mode=mode)

        url_parts = list(urlparse(self._host))
        url_parts[2] = endpoint
        url = urlunparse(url_parts)

        logger.info("using [{}] resolution to request[{}] {}+ candles "
                    "from: {} to: {}".format(precise, url, count,
                                             from_ts, to_ts))
        rsp = requests.get(
            url,
            params={"symbol": self.symbol, "resolution": self._precise_sys,
                    "from": int(round(from_ts.float_timestamp)),
                    "to": int(round(to_ts.float_timestamp))})

        if not rsp.ok:
            raise ValueError(
                "kline host[{}] did't response correctly: {}".format(
                    url, rsp.status_code))

        bar_data = rsp.json()

        if bar_data["s"] != "ok":
            raise ValueError(
                "fail to_ts get history bars: " + json.dumps(bar_data))

        elder_kline = list()
        append_dst = self._kline_cache
        bar_dict = self.__new_bar_data()
        for idx, ts in enumerate(bar_data["t"]):
            ts = arrow.get(ts).to("local")

            if self._kline_cache:
                if ts < self._kline_cache[0].ts:
                    append_dst = elder_kline
                elif self._kline_cache[0].ts <= ts <= self._kline_cache[-1].ts:
                    continue
                else:
                    append_dst = self._kline_cache

            counter = idx % self._precise_multiplier

            bar_dict["volume"] += bar_data["v"][idx]

            bar_dict["high"] = max(bar_dict["high"], bar_data["h"][idx])
            bar_dict["low"] = min(bar_dict["low"], bar_data["l"][idx])

            if counter == 0:
                bar_dict["open"] = bar_data["o"][idx]
                bar_dict["ts"] = ts

            bar_dict["close"] = bar_data["c"][idx]

            if counter == (self._precise_multiplier - 1):
                bar = Bar(**bar_dict)

                if not self._kline_cache or (
                        ts < self._kline_cache[0].ts or
                        ts > self._kline_cache[-1].ts):
                    append_dst.append(bar)

                if trigger:
                    self._cmd_input.put_nowait(
                        self.Command(name="bar", data=bar))

                bar_dict = self.__new_bar_data(pre_bar=bar)

        # to filter out one condition that
        # retrieved bars is elder than kline cache
        if bar_dict["ts"] and bar_dict["ts"] > self._kline_cache[-1].ts:
            self._latest_bar_data = bar_dict

        # latest bar is not finished
        if arrow.now() <= self._latest_bar_data["ts"]:
            self._latest_bar_data = self._kline_cache[-1].to_dict()

        # latest bar will be construct by trade tick
        # to avoid duplicate count of volume, reset volume value
        self._latest_bar_data["volume"] = 0

        # merge with exist kline candles
        self._kline_cache = (
            elder_kline[:self.MAX_KLINE_LEN] +
            self._kline_cache[:self.MAX_KLINE_LEN - len(elder_kline)]
        )

        self._synchronized.set()

    def notify_trade(self, trade_data):
        self._cmd_input.put(
            self.Command(name="trade", data=trade_data)
        )

    def __append_bar(self, bar):
        # last kline cache is not finished in retrieve result
        if bar.ts == self._kline_cache[-1].ts:
            self._kline_cache[-1] = bar
        else:
            self._kline_cache.append(bar)

        self._cmd_input.put_nowait(self.Command(name="bar", data=bar))

    def __confirm_latest_bar(self):
        with self._wait_condition:
            if not self._latest_bar_data["open"]:
                self._latest_bar_data["open"] = self._kline_cache[-1].close
            if not self._latest_bar_data["high"]:
                self._latest_bar_data["high"] = self._kline_cache[-1].close
            if not self._latest_bar_data["low"]:
                self._latest_bar_data["low"] = self._kline_cache[-1].close
            if not self._latest_bar_data["close"]:
                self._latest_bar_data["close"] = self._kline_cache[-1].close

            bar = Bar(**self._latest_bar_data)
            self.__append_bar(bar)

            self._latest_bar_data = self.__new_bar_data(pre_bar=bar)

    def __is_in_bar(self, ts: arrow.arrow.Arrow):
        unit_upgrade_switch = {
            "minute": 60,
            "hour": 24,
            "day": 7
        }

        unit = self.__unit_keywords(self._precise_unit)

        trade_ts_unit_value = getattr(ts, unit)
        latest_bar_ts_unit_value = getattr(self._latest_bar_data["ts"], unit)

        if trade_ts_unit_value < latest_bar_ts_unit_value:
            trade_ts_unit_value += unit_upgrade_switch[unit]

        is_in_bar = (trade_ts_unit_value <
                     latest_bar_ts_unit_value + self._precise_multiplier)

        bar_lag_count = int(round(
            (trade_ts_unit_value - latest_bar_ts_unit_value) /
            self._precise_multiplier))

        return is_in_bar, bar_lag_count

    @condition_waiter(bool_attr="_finished",
                      condition_attr="_wait_condition")
    def __trade_handler(self, trade_data):
        if not self._kline_cache:
            self.retrieve_bars(count=20)

        trade_data["timestamp"] = arrow.get(
            trade_data["timestamp"]/1000).to("local")

        if trade_data["timestamp"] < self._latest_bar_data["ts"]:
            logger.debug(
                "trade tick[{}] is older than kline cache[{}].".format(
                    trade_data["timestamp"], self._latest_bar_data["ts"]
                ))

            return

        is_in_bar, lag_bar_count = self.__is_in_bar(trade_data["timestamp"])

        if not is_in_bar:
            for _ in range(lag_bar_count):
                self.__confirm_latest_bar()

        trade_price = trade_data["price"]
        trade_volume = trade_data["size"]

        # filter out fake trade tick in 1 minute
        # to avoid wrong open price
        if (not trade_price and trade_data["timestamp"].minute ==
                self._latest_bar_data["ts"].minute):
            return

        if not self._latest_bar_data["open"]:
            trade_price = (trade_price if trade_price else
                           self._kline_cache[-1].close)

            self._latest_bar_data["open"] = trade_price
            self._latest_bar_data["high"] = max(
                self._latest_bar_data["high"], trade_price)
            self._latest_bar_data["low"] = min(
                self._latest_bar_data["low"], trade_price)
            self._latest_bar_data["close"] = trade_price
        elif trade_price:
            self._latest_bar_data["high"] = max(
                self._latest_bar_data["high"], trade_price)
            self._latest_bar_data["low"] = min(
                self._latest_bar_data["low"], trade_price)
            self._latest_bar_data["close"] = trade_price

        self._latest_bar_data["volume"] += trade_volume

        logger.info("trade tick: {}, latest bar: {}".format(
            trade_data, self._latest_bar_data))

    @condition_waiter(bool_attr="_finished",
                      condition_attr="_wait_condition")
    def __len__(self):
        return len(self._kline_cache)

    def __iter__(self):
        idx = 0

        length = len(self)

        while idx < length:
            # do not return last cached bar
            # if it's same with latest_bar_data
            # because it's not finished
            if self._kline_cache[idx].ts != self._latest_bar_data["ts"]:
                yield self._kline_cache[idx]

            idx += 1
