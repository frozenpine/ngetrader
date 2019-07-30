# coding: utf-8
import logging
import math
import websocket
import threading
import json

from time import sleep
from urllib.parse import urlparse, urlunparse

from clients.utils import generate_nonce, generate_signature

logger = logging.getLogger(__name__)


# noinspection PyUnusedLocal
class NGEWebsocket(object):
    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self, host, symbol, api_key=None, api_secret=None):
        """
        Connect to the websocket and initialize data stores.
        :param host:
        :param symbol:
        :param api_key:
        :param api_secret:
        """
        logger.debug("Initializing WebSocket.")

        self.endpoint = host
        self.symbol = symbol

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self._api_key = api_key
        self._api_secret = api_secret

        self.data = dict()
        self.keys = dict()
        self.exited = False

        # We can subscribe right in the connection querystring, so let's
        # build that.
        # Subscribe to all pertinent endpoints
        ws_url = self.__get_url()
        logger.info("Connecting to %s" % ws_url)
        self.__connect(ws_url, symbol)
        logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        logger.info('Got all market data. Starting.')

    @property
    def has_authorization(self):
        if self._api_key and self._api_secret:
            return True

        return False

    def exit(self):
        """
        Call this to exit - will close websocket.
        :return:
        """

        self.exited = True
        self.ws.close()

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

    def partial_handler(self, table_name, message):
        logger.debug("%s: partial" % table_name)

        self.data[table_name] = message['data']
        # Keys are communicated on partials to let you know how
        # to uniquely identify
        # an item. We use it for updates.
        self.keys[table_name] = message['keys']

    def insert_handler(self, table_name, message):
        logger.debug(
            '%s: inserting %s' % (table_name, message['data']))

        self.data[table_name] += message['data']

        # Limit the max length of the table to avoid excessive memory usage.
        # Don't trim orders because we'll lose valuable state if we do.
        if table_name not in ['order', 'orderBookL2', "trade"] and len(
                self.data[table_name]) > NGEWebsocket.MAX_TABLE_LEN:
            self.data[table_name] = self.data[table_name][int(
                NGEWebsocket.MAX_TABLE_LEN / 2):]

    def update_handler(self, table_name, message):
        logger.debug(
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

    def delete_handler(self, table_name, message):
        logger.debug(
            '%s: deleting %s' % (table_name, message['data']))

        # Locate the item in the collection and remove it.
        for deleteData in message['data']:
            item = find_item_by_keys(self.keys[table_name],
                                     self.data[table_name], deleteData)
            self.data[table_name].remove(item)

    def __connect(self, ws_url, symbol):
        """
        Connect to the websocket in a thread.
        :param ws_url:
        :param symbol:
        :return:
        """

        logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(ws_url,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException(
                "Could not connect to WS! Exiting.")

    def __get_auth(self):
        """
        Return auth headers. Will use API Keys if present in settings.
        :return:
        """
        if not self.has_authorization:
            logger.info("Not authenticating.")
            return []

        logger.info("Authenticating with API Key.")
        # To auth to the WS using an API key, we generate a signature of
        # a nonce and
        # the WS API endpoint.
        nonce = generate_nonce()
        return [
            "api-nonce: " + str(nonce),
            "api-signature: " + generate_signature(
                self._api_secret, 'GET', '/realtime', nonce, ''),
            "api-key:" + self._api_key
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
        symbol_subs = ["instrument", "orderBookL2", "trade", "quote"]
        if self.has_authorization:
            symbol_subs += ["execution", "order", "position"]
        subscriptions = [sub + ':' + self.symbol for sub in symbol_subs]

        generic_subs = ["margin"]
        if self.has_authorization:
            subscriptions += generic_subs

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
        while not {'margin', 'position', 'order', 'orderBookL2'} <= set(
                self.data):
            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        """
        On subscribe, this data will come down. Wait for it.
        :param symbol:
        :return:
        """
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
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
            logger.warning(
                "parse message failed: {}\n{}".format(e, message))
            return

        if 'subscribe' in message:
            logger.debug("Subscribed to %s." % message['subscribe'])
            return

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
            "partial": self.partial_handler,
            "insert": self.insert_handler,
            "update": self.update_handler,
            "delete": self.delete_handler
        }

        try:
            action_func = action_switch[action]
        except KeyError as e:
            logger.error("Unknown action: %s" % action)
            return

        try:
            action_func(table, message)
        except Exception as e:
            logger.exception(e)

    def __on_error(self, error):
        """
        Called on fatal websocket errors. We exit on these.
        :param error:
        :return:
        """
        if not self.exited:
            logger.error("Error : %s" % error)

            self.exit()

    # noinspection PyMethodMayBeStatic
    def __on_open(self):
        """
        Called when the WS opens.
        :return:
        """
        logger.debug("Websocket Opened.")

    # noinspection PyMethodMayBeStatic
    def __on_close(self):
        """
        Called on websocket close.
        :return:
        """
        logger.info('Websocket Closed')


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
