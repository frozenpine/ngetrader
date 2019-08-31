# coding: utf-8
import os
import yaml
import re
import json

from pprint import pprint
from collections import OrderedDict, namedtuple, ChainMap
from threading import RLock, Condition
from functools import wraps
from bravado.client import ResourceDecorator, CallableOperation

try:
    from common.utils import path, NUM_PATTERN, time_ms
    from common.data_source import CSVData
    from trade.core import Trader
except ImportError:
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

    from common.utils import path, NUM_PATTERN, time_ms
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
        result = self.__REC_DATA_MAPPER[self]

        expect = getattr(self, "expect").strip()

        if not expect:
            return "Pass"

        for key_name, expect_value in [
                [p.strip() for p in exp.strip().split(":")]
                for exp in expect.split(",")]:
            if NUM_PATTERN.match(expect_value):
                expect_value = int(expect_value)

            if result[key_name] != expect_value:
                return "Failed"

        return "Pass"

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
                if NUM_PATTERN.match(ref_idx):
                    ref_idx = int(ref_idx)

                item = item[ref_idx]

        return item

    def data(self):
        data_dict = getattr(self, "to_dict")()

        for fix in ("action", "remark", "expect"):
            data_dict.pop(fix, None)

        for k, v in data_dict.copy().items():
            if not v:
                data_dict.pop(k)

            if isinstance(v, str) and self.__PARAM_PATTERN.match(v):
                data_dict[k] = self.__parse_param(v)

        remark = getattr(self, "remark")
        if remark:
            for key_name, value in [
                [p.strip() for p in rem.strip().split(":")] for
                rem in remark.strip().split(",")
            ]:
                if not self.__PARAM_PATTERN.match(value):
                    continue

                data_dict[key_name] = self.__parse_param(value)

        return data_dict


def notify_rtn(func):
    @wraps(func)
    def waiter(self, *args, **kwargs):
        result = func(self, *args, **kwargs)

        if self.sync_request_rtn:
            with self._wait_condition:
                self._wait_condition.notify()

        return result

    return waiter


class APITester(Trader):
    sync_request_rtn = True
    sync_rtn_wait = 10

    def __init__(self, host="https://www.btcmex.com",
                 symbol="XBTUSD", api_key="", api_secret=""):
        self._request_orders = OrderedDict()

        self._rtn_orders = OrderedDict()
        self._rtn_trade = OrderedDict()

        self._rtn_caches = ChainMap(self._rtn_orders, self._rtn_trade)

        self._wait_condition = Condition(RLock())

        super(APITester, self).__init__(host=host, symbol=symbol,
                                        api_key=api_key,
                                        api_secret=api_secret)

    @notify_rtn
    def on_rtn_order(self, order_data: dict, ts: int = None):
        order_id = order_data["orderID"]

        if order_id not in self._request_orders:
            return

        self._rtn_orders[order_id] = (order_data, ts)

        print("委托回报:")
        pprint(order_data)
        print()

    @notify_rtn
    def on_rtn_trade(self, trade_data: dict, ts: int = None):
        order_id = trade_data["orderID"]

        if order_id not in self._request_orders:
            return

        self._rtn_trade[order_id] = (trade_data, ts)

        print("成交回报:")
        pprint(trade_data)
        print()

    def __getattr__(self, item):
        class ResourceWrapper:
            __args_tuple = namedtuple(
                "args_tuple", ("logger", "req_order_cache", "sync_request_rtn",
                               "wait_condition", "sync_rtn_wait",
                               "rtn_order_cache"))

            def __init__(self, constraints, origin_res, args):
                self.key_name, self.constraints = constraints
                self.args = ResourceWrapper.__args_tuple(*args)
                self.origin_resource = origin_res

            def __check_constraint(self, req_data):
                key_value = req_data[self.key_name]

                if key_value not in self.args.rtn_order_cache:
                    return False

                for constraint_name in self.constraints:
                    constraint_value = req_data[constraint_name]
                    rtn_order, _ = self.args.rtn_order_cache[key_value]
                    if rtn_order[constraint_name] != constraint_value:
                        return False

                return True

            def __call__(self, *args, **kwargs):
                if isinstance(self.origin_resource, CallableOperation):
                    http_future = self.origin_resource(*args, **kwargs)

                    try:
                        req_ts = time_ms()
                        req_results, _ = http_future.result()
                        self.args.logger.info("发送请求 {} : {} {}".format(
                            self.origin_resource.operation.operation_id,
                            args if args else "", kwargs if kwargs else ""))
                        self.args.logger.debug(
                            "url: {}, method: {}, header: {}, data: {}".format(
                                http_future.future.request.url,
                                http_future.future.request.method,
                                http_future.future.request.headers,
                                http_future.future.request.json
                            ))
                    except Exception as e:
                        self.args.logger.exception(e)
                    else:
                        if not isinstance(req_results, list):
                            req_results = [req_results]

                        rtn_results = list()

                        for result in req_results:
                            key_value = result[self.key_name]

                            self.args.req_order_cache[key_value] = result

                            if self.args.sync_request_rtn:
                                with self.args.wait_condition:
                                    if not self.__check_constraint(result):
                                        self.args.wait_condition.wait(
                                            self.args.sync_rtn_wait)

                            try:
                                rtn_result, rtn_ts = self.args.rtn_order_cache[
                                    key_value]
                            except KeyError:
                                pass
                            else:
                                rtn_results.append(rtn_result)

                                self.args.logger.info(
                                    "request[{}] round robin: {} ms".format(
                                        rtn_result,
                                        rtn_ts - req_ts))

                        return (rtn_results if self.args.sync_request_rtn else
                                req_results)

                return self.origin_resource(*args, **kwargs)

            def __getattr__(self, attr_name):
                origin_attr = getattr(self.origin_resource, attr_name)

                if isinstance(origin_attr, (CallableOperation, )):
                    self.origin_resource = origin_attr

                    return self

                return origin_attr

        # todo: 寻找一个 委托 <--> 回报 的唯一性约束，当前约束在特定条件下无效
        constraint_mapper = {
            "Order": ("orderID", ("orderQty", ))
        }

        origin_attribute = getattr(self._rest_client, item)

        if isinstance(origin_attribute, ResourceDecorator):
            return ResourceWrapper(
                constraint_mapper[item], origin_attribute,
                (self.logger, self._request_orders, self.sync_request_rtn,
                 self._wait_condition, self.sync_rtn_wait, self._rtn_orders))

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
    resource_name = str(os.path.basename(order_file).split(".")[0].capitalize())

    order_list = CSVData(order_file, rec_obj_mixin=(DataMixin, ))

    for order in order_list:
        data = order.data()
        # 该 "text" 自定义字段在改单时，会被 clear 修改为“改量”还是“改价”状态
        data.update({"text": "test"})
        resource = getattr(ex, resource_name)
        for order_result in getattr(resource, order.action.strip())(**data):
            order.link(order_result)

    for order in order_list:
        print(json.dumps(order.to_dict()), order.check_result())

    ex.join()
