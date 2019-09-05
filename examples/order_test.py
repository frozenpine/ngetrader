# coding: utf-8

from clients.nge_rest import api


if __name__ == "__main__":
    host = "http://192.168.1.24"
    key = "17YIIfr4otZ9OsmfRX89"
    secret = ("eYH4XMd79c5Gs7s103bvZ5cui4F9QhcPEE2vQkcCmrCjk8HP8LP1"
              "T4MttJ48byolP7aVeQ99LeVz8q0y38L61FNSuN763Mg3T18")

    client = api(host=host, api_key=key, api_secret=secret)

    result, rsp = client.Order.Order_cancelAllAfter(timeout=3600).result()

    print(result, rsp)
