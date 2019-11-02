# coding: utf-8
import os
import sys
import time

from bravado.exception import HTTPTooManyRequests, HTTPServiceUnavailable

try:
    from clients.nge_rest import api
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

    from clients.nge_rest import api

if __name__ == "__main__":
    # fake id
    key = "Y2YzvfLAr0e"
    secret = ("sKNKrI5BTh122tDs4DtHBpy5quM9v05XIpcw6y7s"
              "m0AhT0Lu5nEmq8g43K860CE9IgM6e9xgB7Ffx61bDa")

    client = api(api_key=key, api_secret=secret)

    count = 0
    max_count = 1000
    start = time.time()
    end = 0
    while count < max_count:
        try:
            result, _ = client.Position.Position_get().result()
        except (HTTPTooManyRequests, HTTPServiceUnavailable) as e:
            end = time.time()
            print(e)
            break

        count += 1

    if count >= max_count:
        end = time.time()

    duration = end - start

    print("total request[{:d}] in {:f} seconds with"
          "request rate {:.2f} rps".format(count, duration, count/duration))
