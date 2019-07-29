# coding: utf-8

from bravado.client import SwaggerClient, ResourceDecorator, CallableOperation
from bravado.http_future import HttpFuture

from gevent.threading import Lock
from gevent.lock import BoundedSemaphore

from .nge_rest import api


class NGEClientPool(object):
    class OperationWrapper(object):
        def __init__(self, origin_attr, semaphore):
            self._origin_attr = origin_attr
            self._semaphore = semaphore

        def __call__(self, *args, **kwargs):
            origin_result = self._origin_attr(*args, **kwargs)

            if isinstance(origin_result, HttpFuture):
                return NGEClientPool.OperationWrapper(
                    origin_result, self._semaphore)

            return origin_result

        def __getattr__(self, item):
            origin_attr = getattr(self._origin_attr, item)

            def wrapper(*args, **kwargs):
                try:
                    return origin_attr(*args, **kwargs)
                finally:
                    self._semaphore.release()

            if callable(origin_attr):
                return wrapper

            return origin_attr

    class ResourceWrapper(object):
        def __init__(self, origin_attr, semaphore):
            self._origin_attr = origin_attr
            self._semaphore = semaphore

        def __getattr__(self, item):
            origin_attr = getattr(self._origin_attr, item)

            if isinstance(origin_attr, ResourceDecorator):
                return NGEClientPool.ResourceWrapper(origin_attr,
                                                     self._semaphore)

            if isinstance(origin_attr, CallableOperation):
                return NGEClientPool.OperationWrapper(origin_attr,
                                                      self._semaphore)

            return origin_attr

    def __init__(self, host="https://www.ybmex.com", config=None, size=200):
        self._pool_size = size

        self._instance_list = list()

        ins = api(host=host, config=config)

        self._instance_list.append(ins)

        for _ in range(self._pool_size-1):
            new_instance = SwaggerClient(
                swagger_spec=ins.swagger_spec,
                also_return_response=config.get("also_return_response",
                                                True) if config else True)
            self._instance_list.append(new_instance)

        self._semaphore = BoundedSemaphore(self._pool_size)

        self._lock = Lock()

        self._idx = 0

    def _next_instance(self):
        with self._lock:
            ins = self._instance_list[self._idx % self._pool_size]

            self._idx += 1

            return ins

    def __getattr__(self, item):
        self._semaphore.acquire()

        origin_attr = getattr(self._next_instance(), item)

        if not origin_attr or not isinstance(origin_attr,
                                             ResourceDecorator):
            return origin_attr

        return NGEClientPool.ResourceWrapper(origin_attr, self._semaphore)
