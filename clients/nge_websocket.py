# coding: utf-8
import logging
import math
import websocket
import threading
import json

from time import sleep, time
from threading import Event
from urllib.parse import urlparse, urlunparse

from clients.utils import generate_nonce, generate_signature
from common.utils import time_ms


# noinspection PyUnusedLocal
class NGEWebsocket(object):
    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    BITMEX_COMPATIABLE = False

    def __init__(self, host, symbol, api_key=None, api_secret=None):
        """
        Connect to the websocket and initialize data stores.
        :param host:
        :param symbol:
        :param api_key:
        :param api_secret:
        """
        self.logger = logging.getLogger(__name__)

        self.logger.debug("Initializing WebSocket.")

        self.endpoint = host
        self.symbol = symbol
        self.subscribed = list()

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self._api_key = api_key
        self._api_secret = api_secret

        self.data = dict()
        self.keys = dict()
        self._running_flag = Event()

    @property
    def has_authorization(self):
        if self._api_key and self._api_secret:
            return True

        return False

    @property
    def running(self):
        return self._running_flag.is_set()

    @running.setter
    def running(self, value):
        if value:
            self._running_flag.set()
            ws_url = self.__get_url()
            self.logger.info("Connecting to %s" % ws_url)
            self.__connect(ws_url)
            self.logger.info('Connected to WS.')

            # Connected. Wait for partials
            self.__wait_for_symbol(self.symbol)
            if self._api_key:
                self.__wait_for_account()

            self.logger.info('Got all market data. Starting.')
        else:
            self._running_flag.clear()

            if self.ws:
                self.ws.close()

            if self.wst and self.wst.is_alive():
                try:
                    self.wst.join()
                except RuntimeError:
                    pass

    def join(self, timeout=None):
        self.wst.join(timeout)

    def get_instrument(self):
        """
        Get the raw instrument data for this symbol.
        :return:
        """
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        instrument = self.data['instrument'][0]
        instrument['tickLog'] = int(
            math.fabs(math.log10(instrument['tickSize'])))
        return instrument

    def get_ticker(self):
        """
        Return a ticker object. Generated from quote and trade.
        :return:
        """

        last_quote = self.data['quote'][-1]
        last_trade = self.data['trade'][-1]
        ticker = {
            "last": last_trade['price'],
            "buy": last_quote['bidPrice'],
            "sell": last_quote['askPrice'],
            "mid": (float(last_quote['bidPrice'] or 0) + float(
                last_quote['askPrice'] or 0)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        instrument = self.data['instrument'][0]
        return {k: round(float(v or 0), instrument['tickLog']) for k, v in
                ticker.items()}

    def funds(self):
        """
        Get your margin details.
        :return:
        """
        return self.data['margin'][0]

    def market_depth(self):
        """
        Get market depth (orderbook). Returns all levels.
        :return:
        """
        return self.data['orderBookL2']

    def open_orders(self, clr_id_prefix):
        """
        Get all your open orders.
        :param clr_id_prefix:
        :return:
        """
        orders = self.data['order']
        # Filter to only open orders (leavesQty > 0) and those that we
        # actually placed
        return [o for o in orders if
                str(o['clOrdID']).startswith(clr_id_prefix) and o[
                    'leavesQty'] > 0]

    def recent_trades(self):
        """
        Get recent trades.
        :return:
        """
        return self.data['trade']

    def _partial_handler(self, table_name, message):
        self.logger.debug("%s: partial" % table_name)

        self.data[table_name] = message['data']
        # Keys are communicated on partials to let you know how
        # to uniquely identify
        # an item. We use it for updates.
        self.keys[table_name] = message['keys']

    def _insert_handler(self, table_name, message):
        self.logger.debug(
            '%s: inserting %s' % (table_name, message['data']))

        self.data[table_name] += message['data']

        # Limit the max length of the table to avoid excessive memory usage.
        # Don't trim orders because we'll lose valuable state if we do.
        if table_name not in ('order', 'orderBookL2', "trade") and len(
                self.data[table_name]) > NGEWebsocket.MAX_TABLE_LEN:
            self.data[table_name] = self.data[table_name][int(
                NGEWebsocket.MAX_TABLE_LEN / 2):]

    def _update_handler(self, table_name, message):
        self.logger.debug(
            '%s: updating %s' % (table_name, message['data']))

        # Locate the item in the collection and update it.
        for update_data in message['data']:
            item = find_item_by_keys(self.keys[table_name],
                                     self.data[table_name], update_data)
            if not item:
                return

            item.update(update_data)
            # Remove cancelled / filled orders
            if table_name == 'order' and item['leavesQty'] <= 0:
                self.data[table_name].remove(item)

    def _delete_handler(self, table_name, message):
        self.logger.debug(
            '%s: deleting %s' % (table_name, message['data']))

        # Locate the item in the collection and remove it.
        for deleteData in message['data']:
            item = find_item_by_keys(self.keys[table_name],
                                     self.data[table_name], deleteData)
            self.data[table_name].remove(item)

    def __connect(self, ws_url):
        """Connect to the websocket in a thread.
        """

        self.logger.debug("Starting thread")
        self.ws = websocket.WebSocketApp(ws_url,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or
               not self.ws.sock.connected and
               conn_timeout <= 0):
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.running = False
            raise websocket.WebSocketTimeoutException(
                "Could not connect to WS! Exiting.")

    def __reconnect(self):
        self.data = dict()
        self.keys = dict()

        ws_url = self.__get_url()
        self.logger.info("ReConnecting to %s" % ws_url)

        while self.running:
            try:
                self.__connect(ws_url)
            except websocket.WebSocketException:
                # todo: 随机回退算法进行递增延时重连
                delay = 5
                self.logger.info(
                    "Delay {}s to retry connecting.".format(delay))
                sleep(delay)
            else:
                break

        self.logger.info('ReConnected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(self.symbol)
        if self._api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def __get_auth(self):
        """
        Return auth headers. Will use API Keys if present in settings.
        :return:
        """
        if not self.has_authorization:
            self.logger.info("Not authenticating.")
            return []

        self.logger.info("Authenticating with API Key.")
        # To auth to the WS using an API key, we generate a signature of
        # a nonce and
        # the WS API endpoint.
        nonce = generate_nonce()
        return [
            "api-expires: " + str(nonce),
            "api-signature: " + generate_signature(
                self._api_secret, 'GET',
                '/realtime' if self.BITMEX_COMPATIABLE else '/api/v1/signature',
                nonce, ''),
            "api-key: " + self._api_key
        ]

    def __get_url(self):
        """
        Generate a connection URL. We can define subscriptions
        right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        :return:
        """

        # You can sub to orderBookL2 for all levels, or orderBook10 for top
        # 10 levels & save bandwidth
        symbol_subs = [
            "instrument", "orderBookL2",
            "trade", "quote"] if self.BITMEX_COMPATIABLE else [
            "instrument", "orderBookL2", "trade"]
        if self.has_authorization:
            symbol_subs += [
                "execution", "order",
                "position"] if self.BITMEX_COMPATIABLE else ["order"]

        self.subscribed += symbol_subs
        subscriptions = [sub + ':' + self.symbol for sub in symbol_subs]

        generic_subs = ["margin"] if self.BITMEX_COMPATIABLE else [
            "execution", "position", "margin"]
        if self.has_authorization:
            subscriptions += generic_subs
            self.subscribed += generic_subs

        url_parts = list(urlparse(self.endpoint))
        url_parts[0] = url_parts[0].replace('http', 'ws')
        url_parts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))

        return urlunparse(url_parts)

    def __wait_for_account(self):
        """
        On subscribe, this data will come down. Wait for it.
        :return:
        """
        # Wait for the keys to show up from the ws
        wait_tables = {'margin', 'order', 'position'} & set(self.subscribed)

        while True:
            retrieved_account = wait_tables & set(self.data)

            if len(retrieved_account) == len(wait_tables):
                break

            self.logger.debug(
                "Account table retrieved: {}".format(retrieved_account))

            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        """
        On subscribe, this data will come down. Wait for it.
        :param symbol:
        :return:
        """
        wait_tables = {'instrument', 'trade',
                       'quote', 'orderBookL2'} & set(self.subscribed)

        while True:
            retrieved_symbols = wait_tables & set(self.data)

            if len(retrieved_symbols) == len(wait_tables):
                break

            self.logger.debug(
                "Symbol table retrieved: {}".format(retrieved_symbols))

            sleep(0.1)

    def __send_command(self, command, args=None):
        """
        Send a raw command.
        :param command:
        :param args:
        :return:
        """
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __on_message(self, message):
        """
        Handler for parsing WS messages.
        :param message:
        :return:
        """

        try:
            message = json.loads(message)
        except ValueError as e:
            self.logger.warning(
                "parse message failed: {}\n{}".format(e, message))
            return

        if 'subscribe' in message:
            self.logger.debug("Subscribed to %s." % message['subscribe'])
            return

        message["@timestamp"] = time_ms()

        table = message.get('table')
        action = message.get('action')

        if not action:
            return

        # There are four possible actions from the WS:
        # 'partial' - full table image
        # 'insert'  - new row
        # 'update'  - update row
        # 'delete'  - delete row
        action_switch = {
            "partial": self._partial_handler,
            "insert": self._insert_handler,
            "update": self._update_handler,
            "delete": self._delete_handler
        }

        try:
            action_func = action_switch[action]
        except KeyError as e:
            self.logger.error("Unknown action: %s" % action)
            return

        try:
            action_func(table, message)
        except Exception as e:
            self.logger.exception(e)

    def __on_error(self, error):
        """
        Called on fatal websocket errors. We exit on these.
        :param error:
        :return:
        """
        if not self.running:
            self.logger.error("Error : %s" % error)

    def __on_open(self):
        """
        Called when the WS opens.
        :return:
        """
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        """
        Called on websocket close.
        :return:
        """
        self.logger.info('Websocket Closed')

        if self.running:
            self.__reconnect()


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out
# which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema),
# we have a "keys" array. These are the
# fields we can use to uniquely identify an item.
# Sometimes there is more than one, so we iterate through all
# provided keys.
def find_item_by_keys(keys, table_data, match_data):
    for item in table_data:
        matched = True
        for key in keys:
            if item[key] != match_data[key]:
                matched = False
        if matched:
            return item
