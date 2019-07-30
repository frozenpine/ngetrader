# coding: utf-8

import hmac
import hashlib
import requests

from time import time
from functools import wraps


def generate_nonce(expire=5):
    return int(round(time()) + expire)


def generate_signature(secret, verb, url, nonce, data):
    """Generate a request signature compatible with BitMEX."""
    # Parse the url so we can remove the base and extract just the path.
    # noinspection PyUnresolvedReferences
    parsed_url = urllib.parse.urlparse(url)
    path = parsed_url.path
    if parsed_url.query:
        path = path + '?' + parsed_url.query

    # print "Computing HMAC: %s" % verb + path + str(nonce) + data
    message = (verb + path + str(nonce) + data).encode('utf-8')

    signature = hmac.new(secret.encode('utf-8'), message,
                         digestmod=hashlib.sha256).hexdigest()
    return signature

def validate_price(price):
    return price != 0


def validate_volume(volume):
    return volume > 0


def http_request(uri, method="POST", session=None, **kwargs):
    if not session:
        session = requests.Session()

    response = getattr(session, method.lower())(
        uri, **kwargs)

    if not response.ok:
        raise requests.HTTPError(response.text)

    return response


def condition_controller(bool_attr: str, condition_attr: str):
    def controller(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            condition = getattr(self, condition_attr)

            with condition:
                setattr(self, bool_attr, False)

                result = func(self, *args, **kwargs)

                setattr(self, bool_attr, True)

                condition.notify()

                return result

        return wrapper

    return controller


def condition_waiter(bool_attr: str, condition_attr: str):
    def waiter(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            condition = getattr(self, condition_attr)

            with condition:
                condition.wait_for(lambda: getattr(self, bool_attr))

                result = func(self, *args, **kwargs)

                return result

        return wrapper

    return waiter
