# coding: utf-8
import os
import yaml
import re
import json

from collections import OrderedDict, namedtuple, ChainMap, UserDict
from threading import RLock, Condition
from functools import wraps
from bravado.client import ResourceDecorator, CallableOperation
from bravado.exception import HTTPBadRequest

try:
    from common.utils import (path, time_ms, TColor,
                              try_parse_num, try_parse_regex)
    from common.data_source import CSVData
    from trade.core import Trader
except ImportError:
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

    from common.utils import (path, time_ms, TColor,
                              try_parse_num, try_parse_regex)
    from common.data_source import CSVData
    from trade.core import Trader


class DataMixin(object):
    __REC_DATA_MAPPER = OrderedDict()

    __PARAM_PATTERN = re.compile(r'@.+@')
    __TABLE_PATTERN = re.compile(r'#.+#')

    def link(self, result_data):
        self.__REC_DATA_MAPPER[self] = result_data

    def last_order(self):
        last_key = [k for k in self.__REC_DATA_MAPPER.keys() if k != self][-1]

        return self.__REC_DATA_MAPPER[last_key]

    def order_list(self):
        return list(self.__REC_DATA_MAPPER.values())

    def check_result(self):
        result = self.__REC_DATA_MAPPER.get(self, None)

        expect = getattr(self, "expect")

        if not expect or not result:
            return True, result

        if "Error" in expect:
            err_expect = expect.split(":")[1].strip()

            parsed, regex = try_parse_regex(err_expect)
            if not parsed:
                return err_expect in result["error"]["message"], result

            if regex.findall(result["error"]["message"]):
                return True, result

            return False, result

        for key_name, expect_value in [
                [p.strip() for p in exp.strip().split(":")]
                for exp in expect.split(",")]:
            _, expect_value = try_parse_num(expect_value)

            try:
                result_value = result[key_name]
            except (KeyError, AttributeError, TypeError):
                return False, result

            if result_value != expect_value:
                return False, result

        return True, result

    def highlight_check_result(self):
        result = self.__REC_DATA_MAPPER.get(self, None)

        result_string = (json.dumps(result, ensure_ascii=False)
                         if isinstance(result, dict) else str(result))

        expect = getattr(self, "expect")

        if not expect or not result:
            return True, result_string

        if "Error" in expect:
            err_expect = expect.split(":")[1].strip()

            parsed, regex = try_parse_regex(err_expect)

            if not parsed:
                return err_expect in result_string, result_string.replace(
                    err_expect, TColor.highlight(err_expect, "green")
                )

            matched = False

            for msg in regex.findall(result_string):
                result_string = result_string.replace(
                    msg, TColor.highlight(msg, "green"))
                matched = True

            return matched, result_string

        check_bool = True

        for key_name, expect_value in [
                [p.strip() for p in exp.strip().split(":")]
                for exp in expect.split(",")]:
            _, expect_value = try_parse_num(expect_value)

            try:
                value_match = expect_value == result[key_name]

                highlight_value = json.dumps(
                    {key_name: result[key_name]}).strip("{}")
            except (KeyError, AttributeError, TypeError):
                check_bool = False
                result_string = TColor.highlight(result_string, "red")
            else:
                result_string = result_string.replace(
                    highlight_value,
                    TColor.highlight(highlight_value,
                                     "green" if value_match else "red"))

                check_bool = check_bool and value_match

        return check_bool, result_string

    def __parse_param(self, value):
        item_pattern = re.compile(
            r'\$?{?([0-9a-zA-Z_-]+)(?:\[([0-9a-zA-Z_-]+)\])?}?')

        item_chain = value.strip('@').split(".")

        item = self

        for ref_name, ref_idx in [item_pattern.match(i).groups()
                                  for i in item_chain]:
            try:
                item = getattr(item, ref_name)
            except AttributeError:
                item = item[ref_name]

            if callable(item):
                item = item()

            if ref_idx is not None:
                _, ref_idx = try_parse_num(ref_idx)

                item = item[ref_idx]

        return item

    def data(self):
        data_dict = getattr(self, "to_dict")()

        for fix in ("action", "params", "expect"):
            data_dict.pop(fix, None)

        for k, v in data_dict.copy().items():
            if not isinstance(v, str):
                if v is None:
                    data_dict.pop(k)

                continue

            if not v:
                data_dict.pop(k)

            if self.__PARAM_PATTERN.match(v):
                data_dict[k] = self.__parse_param(v)

        params = getattr(self, "params")
        if params:
            for key_name, value in [
                [p.strip() for p in param.strip().split(":")] for param in
                params.split(",")
            ]:
                if not self.__PARAM_PATTERN.match(value):
                    continue

                data_dict[key_name] = self.__parse_param(value)

        return data_dict

    def do_action(self, act_resource):
        action = getattr(self, "action")
        data = self.data()

        for order_result in getattr(act_resource, action)(**data):
            self.link(order_result)


def notify_rtn(wait_flag, wait_condition):
    def waiter(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)

            if getattr(self, wait_flag):
                wait = getattr(self, wait_condition)
                with wait:
                    wait.notify()

            return result

        return wrapper

    return waiter


class RequestCache(UserDict):
    def __init__(self):
        super(RequestCache, self).__init__()

        self._inflight_cache = OrderedDict()
        self._request_cache = OrderedDict()

        self._cache = ChainMap(self._inflight_cache, self._request_cache)

    def is_inflight(self, key):
        return key in self._inflight_cache

    def inflight(self, key, value):
        self._request_cache[key] = self._inflight_cache[key] = value

    def __setitem__(self, key, value):
        if key not in self._inflight_cache and key not in self._request_cache:
            return self.inflight(key, value)

        if key in self._inflight_cache:
            self._inflight_cache.pop(key)

        self._request_cache[key] = value

    def __getitem__(self, item):
        return self._cache[item]

    def __getattr__(self, item):
        return self._cache[item]

    def __delitem__(self, key):
        if key in self._inflight_cache:
            del self._inflight_cache[key]

        del self._request_cache[key]

    def __len__(self):
        return len(self._cache)

    def __contains__(self, item):
        return item in self._cache

    def __iter__(self):
        return self._cache.__iter__()


class ResourceWrapper:
    args_tuple = namedtuple(
        "args_tuple", ("sync_req_rtn", "wait_condition",
                       "rtn_wait_timeout"))

    __EXECUTION_PATTERN = re.compile(r'{%.+%}')

    def __init__(self, req_key: str, req_cache: RequestCache,
                 rtn_cache, logger, origin_res, args: args_tuple):
        self.key_name = req_key
        self.req_cache = req_cache
        self.rtn_cache = rtn_cache
        self.logger = logger
        self.args = args
        self.origin_resource = origin_res
        self.origin_operation = None

    def __execution(self, http_future):
        req_ts = time_ms()

        req_results, rsp = http_future.result()
        self.logger.info(
            "send request[{}]: url[{}], method[{}], "
            "header[{}], data[{}]".format(
                http_future.operation.operation_id,
                http_future.future.request.url,
                http_future.future.request.method,
                http_future.future.request.headers,
                (http_future.future.request.data if
                 http_future.future.request.data else
                 json.dumps(http_future.future.request.json)
                 )
            )
        )

        if not isinstance(req_results, list):
            req_results = [req_results]

        return req_ts, req_results

    def __call__(self, *args, **kwargs):
        if not self.origin_operation:
            raise TypeError("Operation is invalid.")

        http_future = self.origin_operation(*args, **kwargs)

        try:
            req_ts, req_results = self.__execution(http_future)
        except HTTPBadRequest as e:
            if e.swagger_result:
                return [e.swagger_result]
            else:
                raise

        if not self.args.sync_req_rtn:
            return req_results

        rtn_results = list()

        for result in req_results:
            key_value = result[self.key_name]

            self.req_cache.inflight(key_value, result)

            with self.args.wait_condition:
                wait_count = 0

                while self.req_cache.is_inflight(key_value):
                    self.args.wait_condition.wait(
                        self.args.rtn_wait_timeout)

                    wait_count += 1

                    if wait_count > 3:
                        raise RuntimeError(
                            "Waiting response timeout "
                            "for {} times[{} s]".format(
                                wait_count, self.args.rtn_wait_timeout))

            try:
                rtn_result, rtn_ts = self.rtn_cache[key_value]
            except KeyError:
                self.logger.error(
                    "fail to get rtn[{}] from cache: {}".format(
                        key_value, self.rtn_cache
                    ))
                continue

            rtn_results.append(rtn_result)

            if rtn_ts:
                self.logger.info(
                    "receive response in [{} ms]: {}".format(
                        rtn_ts - req_ts, rtn_result))

        return rtn_results

    def __getattr__(self, attr_name):
        origin_attr = getattr(self.origin_resource, attr_name)

        if isinstance(origin_attr, (CallableOperation, )):
            self.origin_operation = origin_attr

            return self

        return origin_attr


class APITester(Trader):
    SYNC_REQ_WITH_RTN = True
    RTN_WAIT_TIMEOUT = 10

    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        self._request_cache = RequestCache()

        self._rtn_order_cache = OrderedDict()
        self._rtn_trade_cache = OrderedDict()

        self._rtn_caches = ChainMap(self._rtn_order_cache,
                                    self._rtn_trade_cache)

        self._wait_condition = Condition(RLock())

        super(APITester, self).__init__(host=host, symbol=symbol,
                                        api_key=api_key,
                                        api_secret=api_secret)
        self.logger = logging.getLogger(__file__)

    @notify_rtn(wait_flag="SYNC_REQ_WITH_RTN",
                wait_condition="_wait_condition")
    def on_rtn_order(self, order_data: dict, ts: int = None):
        order_id = order_data["orderID"]

        if order_id not in self._request_cache:
            return

        self._rtn_order_cache[order_id] = (order_data, ts)
        self._request_cache[order_id] = order_data

    @notify_rtn(wait_flag="SYNC_REQ_WITH_RTN",
                wait_condition="_wait_condition")
    def on_rtn_trade(self, trade_data: dict, ts: int = None):
        order_id = trade_data["orderID"]

        if order_id not in self._request_cache:
            return

        self._rtn_trade_cache[order_id] = (trade_data, ts)

    def __getattr__(self, item):
        key_mapper = {
            "Order": "orderID"
        }
        rtn_cache_mapper = {
            "Order": self._rtn_order_cache
        }

        origin_attribute = getattr(self._rest_client, item)

        if isinstance(origin_attribute, ResourceDecorator):
            # noinspection PyCallByClass
            return ResourceWrapper(
                req_key=key_mapper[item], req_cache=self._request_cache,
                rtn_cache=rtn_cache_mapper[item], logger=self.logger,
                origin_res=origin_attribute,
                args=ResourceWrapper.args_tuple(
                    sync_req_rtn=self.SYNC_REQ_WITH_RTN,
                    wait_condition=self._wait_condition,
                    rtn_wait_timeout=self.RTN_WAIT_TIMEOUT))

        return origin_attribute


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    config_base = path("./config")
    csv_base = path("./csv")

    with open(os.path.join(config_base, "order_config.yml"), mode='rb') as f:
        config = yaml.safe_load(f)

    ex = APITester(**config)

    order_file = os.path.join(csv_base, "order.csv")
    resource_name = str(os.path.basename(order_file).split(
        ".")[0].capitalize())
    resource = getattr(ex, resource_name)

    order_list = CSVData(order_file, rec_obj_mixin=(DataMixin, ))

    for order in order_list:
        order.do_action(resource)

    print()

    for idx, order in enumerate(order_list):
        passed, highlight_result = order.highlight_check_result()

        input_string = "{:02d}. Input: ".format(idx + 1)

        print((
                input_string +
                "{}\n".format(json.dumps(order.to_dict(),
                                         ensure_ascii=False)) +
                "Return: ".rjust(len(input_string)) +
                "{}\n".format(highlight_result) +
                "Result: ".rjust(len(input_string)) +
                (TColor.rend("Pass", fg="green") if passed else
                 TColor.rend("Failed", fg="red"))
        ).replace(
            "Input", TColor.rend("Input", fg="yellow")).replace(
            "Return", TColor.rend("Return", fg="blue")).replace(
            "Result", TColor.rend("Result", fg="cyan"))
        )
