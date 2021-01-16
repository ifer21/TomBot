from __future__ import absolute_import
from time import sleep
import time
import sys
from datetime import datetime
from os.path import getmtime
import random
import requests
import atexit
import signal
import pandas as pd
import numpy as np
from builtins import any as b_any
from market_maker import bitmex
from market_maker.settings import settings
from market_maker.utils import log, constants, errors, math, plot_utiles, telegram_bot, poscals
from btmex_data import get_bitmex_data

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


#
# Helpers
#
logger = log.setup_custom_logger('root')


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY,
                                    timeout=settings.TIMEOUT)

        self.bitmex.isolate_margin(self.symbol, settings.LEVERAGE)

    def cancel_order(self, order):
        tickLog = self.get_instrument()['tickLog']
        logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
        while True:
            try:
                self.bitmex.cancel(order['orderID'])
                sleep(settings.API_REST_INTERVAL)
            except ValueError as e:
                logger.info(e)
                sleep(settings.API_ERROR_INTERVAL)
            else:
                break

    def cancel_all_orders(self):
        if self.dry_run:
            return
        tickLog = self.get_instrument()['tickLog']
        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()
        # Only cancel all orders (inlcuding SL) if no position is open
        if self.get_position()['currentQty'] == 0:
            logger.info("Resetting current position. Canceling all existing orders.")
            if len(orders):
                self.bitmex.cancel([order['orderID'] for order in orders])
        else:
            logger.info("Resetting current position. Leaving only SL and TP orders.")
            orderstocancel = []
            for order in orders:
                #if order['ordType'] != 'Stop':  # We dont want the Stop Loss or Take Profit orders to be canceled
                if ('SBuy' in order['clOrdID']) or ('SSell' in order['clOrdID']):
                    orderstocancel.append(order)
                elif (order['ordType'] != 'Stop') and ('Tp' not in order['clOrdID']):
                    orderstocancel.append(order)
                    #logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            if len(orderstocancel):
                self.bitmex.cancel([order['orderID'] for order in orderstocancel])

        sleep(settings.API_REST_INTERVAL)


    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.bitmex.position(symbol=symbol)
            instrument = self.bitmex.instrument(symbol=symbol)

            if instrument['isQuanto']:
                future_type = "Quanto"
            elif instrument['isInverse']:
                future_type = "Inverse"
            elif not instrument['isQuanto'] and not instrument['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % instrument['symbol'])

            if instrument['underlyingToSettleMultiplier'] is None:
                multiplier = float(instrument['multiplier']) / float(instrument['quoteToSettleMultiplier'])
            else:
                multiplier = float(instrument['multiplier']) / float(instrument['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(instrument['markPrice']),
                "spot": float(instrument['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def get_instrument(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.instrument(symbol)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    def get_orders(self, symbol):
        if self.dry_run:
            return []
        return self.bitmex.open_orders(symbol)

    def get_highest_buy(self, symbol):
        buys = [o for o in self.get_orders(symbol) if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self, symbol):
        sells = [o for o in self.get_orders(symbol) if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.position(symbol)

    def get_ticker(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.ticker_data(symbol)

    def is_open(self):
        """Check that websockets are still open."""
        return not self.bitmex.ws.exited

    def check_market_open(self):
        instrument = self.get_instrument()
        if instrument["state"] != "Open" and instrument["state"] != "Closed":
            raise errors.MarketClosedError("The instrument %s is not open. State: %s" %
                                           (self.symbol, instrument["state"]))

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        instrument = self.get_instrument()
        if instrument['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")

    def amend_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.amend_bulk_orders(orders)

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.create_bulk_orders(orders)

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        if len(orders)==0:
            return orders
        else:
            return self.bitmex.cancel([order['orderID'] for order in orders])

    def get_trades(self):
        # Executed trades from people
        return self.bitmex.recent_trades()

    def executed_orders(self):
        # Executed trades from people
        return self.bitmex.executed_orders()

    def all_orders(self):
        # all orders
        return self.bitmex.all_orders()

    def filled_orders(self):
        # Websocket
        # Filled orders in this run
        return self.bitmex.filled_orders()

    def filled_orders_hist(self):
        # Rest API
        # Return filled orders historical
        return self.bitmex.filled_orders_hist()

    def last_filled_orders_hist(self):
        # Rest API Last filled order in the tradeHistory
        return self.bitmex.last_filled_orders_hist()

    def filled_orders_hist_count(self, count):
        return self.bitmex.filled_orders_hist_count(count)

    def filled_orders_hist_count_symbol(self, symbol='XBTUSD', count=20):
        return self.bitmex.filled_orders_hist_count_symbol(symbol, count)

    def last_filled_orders_hist_symbol(self, symbol='XBTUSD', count=20):
        """ Return first last filled order which is has a clorID != '' and symvol """
        lforder = self.bitmex.filled_orders_hist_count_symbol(symbol, count)
        if lforder:
            last = next((forder for forder in lforder if forder['symbol'] == symbol and forder['clOrdID'] != ''), False)
        else:
            last = False
        return last


class OrderManager:
    def __init__(self):
        self.exchange = ExchangeInterface(settings.DRY_RUN)
        # Once exchange is created, register exit handler that will always cancel orders
        # on any error.
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        logger.info("Using symbol %s." % self.exchange.symbol)
        logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        self.start_time = datetime.now()
        self.instrument = self.exchange.get_instrument()
        self.starting_qty = self.exchange.get_delta()
        self.running_qty = self.starting_qty
        self.reset()

    def reset(self):
        #self.exchange.cancel_all_orders()
        self.sanity_check()
        self.last_mark_price = self.print_status()

        # Create orders and converge.
        self.place_orders()

    def print_status(self):
        """Print the current MM status."""

        margin = self.exchange.get_margin()
        position = self.exchange.get_position()
        self.running_qty = self.exchange.get_delta()
        tickLog = self.exchange.get_instrument()['tickLog']
        self.start_XBt = margin["marginBalance"]
        self.f_orders = self.exchange.last_filled_orders_hist()  # Gets last filled trade
        self.filled_df = self.update_last_filled(self.f_orders)

        logger.info("Current XBT Balance: %.6f" % XBt_to_XBT(self.start_XBt))
        logger.info("Current Contract Position: %d" % self.running_qty)
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Cost Price: %.*f" % (tickLog, float(position['avgCostPrice'])))
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.exchange.calc_delta()['spot'])

        return self.exchange.get_instrument()['markPrice']


    def update_last_filled(self, f_orders):
        # Updating filled orders
        path_trend = '/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/'
        #filled_df_new = pd.json_normalize(self.exchange.update_filled())
        filled_df_new = pd.DataFrame(f_orders)
        if len(filled_df_new) != 0:
            if not os.path.isfile(path_trend + 'historic/last_fills_py.csv'):
                # Building historical fills if first time
                filled_df_new.to_csv(path_trend + 'historic/last_fills_py.csv', sep='\t', index=False)
                return filled_df_new
            else:
                # Reading previous historical fills
                filled_df = pd.read_csv(path_trend + 'historic/last_fills_py.csv', sep='\t')
                # Merging new executed trades
                print('++++++++++++++++ UPDATE LAST FILLS_PY ++++++++++++++')
                filled_df = pd.concat([filled_df, filled_df_new], ignore_index=True, sort=False).drop_duplicates(
                    ['orderID', 'orderQty', 'price'], keep='first')
                filled_df.to_csv(path_trend + 'historic/last_fills_py.csv', sep='\t', index=False)
                return filled_df
        else:
            return pd.DataFrame()

    def get_trendlines(self, symbol):
        logger.info("Reading information for trendlines")
        path_trend = '/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/'
        trends_df = pd.read_excel(path_trend+'trendlines.xlsx', header=0)
        # Subsetting to speciefied symbol
        trends_df = trends_df[trends_df['symbol'] == symbol]
        trends_df.reset_index(inplace=True)
        for i, row in trends_df.iterrows():
            trends_df.loc[i, 'time_low_ts'] = datetime.strptime(row['time_low'], "%Y-%m-%d %H:%M:%S")
            trends_df.loc[i, 'time_low_ts_iso'] = trends_df.loc[i, 'time_low_ts'].isoformat()+'.000Z'
            trends_df.loc[i, 'time_high_ts'] = datetime.strptime(row['time_high'], "%Y-%m-%d %H:%M:%S")
            trends_df.loc[i, 'time_high_ts_iso'] = trends_df.loc[i, 'time_high_ts'].isoformat()+'.000Z'
            try:
                trends_df.loc[i, 'slope'] = (trends_df.loc[i, 'price_high']-trends_df.loc[i, 'price_low'])/(time.mktime(trends_df.loc[i, 'time_high_ts'].timetuple())-time.mktime(trends_df.loc[i, 'time_low_ts'].timetuple()))
                trends_df.loc[i, 'interc'] = trends_df.loc[i, 'price_low']-(trends_df.loc[i, 'slope']*time.mktime(trends_df.loc[i, 'time_low_ts'].timetuple()))
            except TypeError:
                logger.error(f"Wrong input in trendlines.xlsx: {trends_df.loc[i]}")
                #raise TypeError(f"Wrong input in trendlines.xlsx: {trends_df.loc[i]}")
        self.trendlines = trends_df
        trends_df.to_csv(path_trend + 'trendlines_'+symbol+'_py.csv', sep='\t', index=False)
        return trends_df

    def order_mover(self, slope, b):
        ### Price prediction from a trendline (y=mx+b) given a timestamp
        timestamp = self.exchange.get_instrument()['timestamp'] # current timestamp
        time_or = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        time_ts = time.mktime(time_or.timetuple())
        price_pred = slope*time_ts+b
        #print(f'\n\n{slope:1.3f} {b:1.2f} {timestamp} {price_pred}')
        return price_pred

    def wakup_timer(self, time):
        self.wakeup_time += time
        return self.wakeup_time

    def get_ticker(self):
        ticker = self.exchange.get_ticker()
        tickLog = self.exchange.get_instrument()['tickLog']
        
        trends_df = self.get_trendlines(self.exchange.symbol)
        for i, row in self.trendlines.iterrows():
            self.trendlines.loc[i, 'price_pred'] = self.order_mover(row['slope'], row['interc'])
            logger.info(f"Precicted price for {row['trend_name']}: {self.order_mover(row['slope'], row['interc']):1.2f} ")

        # Set up our buy & sell positions as the smallest possible unit above and below the current spread
        # and we'll work out from there. That way we always have the best price but we don't kill wide
        # and potentially profitable spreads.
        #self.start_position_buy = ticker["buy"] + self.instrument['tickSize']
        #self.start_position_sell = ticker["sell"] - self.instrument['tickSize']

        # Back off if our spread is too small.
        #if self.start_position_buy * (1.00 + settings.MIN_SPREAD) > self.start_position_sell:
        #    self.start_position_buy *= (1.00 - (settings.MIN_SPREAD / 2))
        #    self.start_position_sell *= (1.00 + (settings.MIN_SPREAD / 2))#

        # Midpoint, used for simpler order placement.
        self.start_position_mid = ticker["mid"]
        logger.info(
            "%s Ticker: Buy: %.*f, Sell: %.*f" %
            (self.instrument['symbol'], tickLog, ticker["buy"], tickLog, ticker["sell"])
        )
        #logger.info('Start Positions: Buy: %.*f, Sell: %.*f, Mid: %.*f' %
        #            (tickLog, self.start_position_buy, tickLog, self.start_position_sell,
        #             tickLog, self.start_position_mid))
        return ticker

    def get_price_offset(self, index):
        """Given an index (1, -1, 2, -2, etc.) return the price for that side of the book.
           Negative is a buy, positive is a sell."""
        # Maintain existing spreads for max profit
        if settings.MAINTAIN_SPREADS:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # First positions (index 1, -1) should start right at start_position, others should branch from there
            index = index + 1 if index < 0 else index - 1
        else:
            # Offset mode: ticker comes from a reference exchange and we define an offset.
            start_position = self.start_position_buy if index < 0 else self.start_position_sell

            # If we're attempting to sell, but our sell price is actually lower than the buy,
            # move over to the sell side.
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # Same for buys.
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy
        
        return math.toNearest(start_position * (1 + settings.INTERVAL) ** index, self.instrument['tickSize'])

    ###
    # Orders
    ###


    def position_printer(self):

        self.account_margin = self.exchange.get_margin()['amount']  # Total Account Marg in (XBt)
        self.available_margin = self.exchange.get_margin()['availableMargin']  # Available Margin Balance (XBt)
        self.fundingrate = self.exchange.get_instrument()['fundingRate']
        self.wspos = self.exchange.get_position()
        self.leverage = self.wspos['leverage']
        self.pos_margin = self.wspos['posMargin']  # Position Margin (XBt)
        self.pos_maint_margin = self.wspos['maintMargin']  # Maintenance margin XBt (este es el que sale en bitmex)
        self.current_position = self.wspos['currentQty']  # The current position amount in contracts. (contracts)
        self.current_value_XBT = self.wspos['homeNotional']  # Position Value in XBT
        self.pos_cost = self.wspos['grossOpenCost']  # grossOpenCost: The absolute of your open orders for this symbol. (XBt)
        self.pos_realPnL = self.wspos['realisedPnl']  # Since last entry
        self.pos_uPnL_roe = self.wspos['unrealisedRoePcnt']
        self.pos_uPnL = self.wspos['unrealisedPnl']  # Position uPnL (XBt)
        self.pos_uPnLpercen = self.wspos['unrealisedPnlPcnt']
        self.pos_PnL = self.wspos['realisedGrossPnl']
        self.pos_liqprice = self.wspos['liquidationPrice']
        self.pos_markprice = self.wspos['markPrice']
        self.pos_lastprice = self.wspos['lastPrice']
        self.pos_avgentry = self.wspos['avgEntryPrice']
        self.pos_beven = self.wspos['breakEvenPrice']
        self.volume = self.exchange.get_instrument()['volume']

    def messenger(self, wake_up_time):
        # Sending message to telegram bot
        if wake_up_time % 300 == 0:
            message = f"*Account Balance:* {np.round(XBt_to_XBT(self.account_margin), 4)}XBT  *Available Balance:* {np.round(XBt_to_XBT(self.available_margin), 4)}XBT\n"
            if self.pos_margin:
                message = message + f"*Size:* {self.current_position} \t *Value:* {np.round(self.current_value_XBT, 4)}XBT\n"
                message = message + f"Current open position: {np.round(100 * self.pos_margin / self.account_margin, 1)}% of account\n"
                message = message + f"*Entry Price:* {np.round(self.pos_avgentry, 2)}USD \t *Mark Price:* {np.round(self.pos_markprice, 2)}XBT \t *Liq. Price:* {np.round(self.pos_liqprice, 2)}XBT\n"
                message = message + f"*Bid Price:* {np.round(self.exchange.get_instrument()['bidPrice'],2)} \t *Ask Price:* {np.round(self.exchange.get_instrument()['askPrice'],2)}\n"
                message = message + f"*Margin:* {np.round(XBt_to_XBT(self.pos_margin), 4)}XBT \t *Maint. Margin:* {np.round(XBt_to_XBT(self.pos_maint_margin), 4)}XBT\n"
                message = message + f"*Upnl:* \t {np.round(XBt_to_XBT(self.pos_uPnL), 4)}XBT ({np.round(100 * self.pos_uPnL_roe, 2)}%) \t *rPNL:* {np.round(XBt_to_XBT(self.pos_realPnL), 4)}XBT\n"
                message = message + f"+++++++++++++++++++++++++++++++++++"
            telegram_bot.telegram_bot_sendtext(message)
        if self.volume > 60 * 1e6:
            telegram_bot.telegram_bot_sendtext(f"High Volume Alert!\nVolume={self.volume}")

    def chart_plotter(self, path, symbol, wake_up_time=10):
        if not os.path.exists(path + 'figures/'):
            os.makedirs(path + 'figures/')
        if wake_up_time != 0:
            if wake_up_time % 86400 == 0:  # Updating daily chart
                data_5m, data_1h, data_1d, trends_df_pl = plot_utiles.historic_reader(path, self.exchange.symbol)
                fig = plot_utiles.c_plotter('1d', data_5m, data_1h, data_1d, trends_df_pl, self.filled_df)
                fig.write_html(path + 'figures/' + symbol + '_1d_plot.html')
            if wake_up_time % 3600 == 0:  # Updating hourly chart
                data_5m, data_1h, data_1d, trends_df_pl = plot_utiles.historic_reader(path, self.exchange.symbol)
                fig = plot_utiles.c_plotter('1h', data_5m, data_1h, data_1d, trends_df_pl, self.filled_df)
                fig.write_html(path + 'figures/' + symbol + '_1h_plot.html')
            if wake_up_time % 300 == 0:  # Updating 5min chart
                data_5m, data_1h, data_1d, trends_df_pl = plot_utiles.historic_reader(path, self.exchange.symbol)
                fig = plot_utiles.c_plotter('5m', data_5m, data_1h, data_1d, trends_df_pl, self.filled_df)
                fig.write_html(path + 'figures/' + symbol + '_5m_plot.html')

    def cancel_limit_orders(self):
        """ Cancel limit orders on the other side after entering a position (not the tp or SL orders) """
        existing_orders = self.exchange.get_orders(self.exchange.symbol)
        cancel_orders = []
        for order in existing_orders:
            # Not cancelling the breakout orders
            if ('SSell' not in order['clOrdID'].split(' ')[0]) and ('SBuy' not in order['clOrdID'].split(' ')[0]):
                if ('Tp' not in order['clOrdID'].split(' ')[0]) and ('SL' not in order['clOrdID'].split(' ')[0]):
                    cancel_orders.append(order)
        if len(cancel_orders)>0:
            logger.info(f"Cancelling existing limit orders")
            #print(cancel_orders)
            self.exchange.cancel_bulk_orders(cancel_orders)

    def place_orders(self, wake_up_time=10):
        """Create order items for use in convergence."""
        path = '/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/'
        # Plotting charts
        self.chart_plotter(path=path, symbol=self.exchange.symbol, wake_up_time=wake_up_time)
        # Getting position information
        self.position_printer()
        # Sending message on Telegram
        self.messenger(wake_up_time)
        # Updating price orders following trendlines
        logger.info(f"Current open position: {np.round(100 * self.pos_margin / self.account_margin, 1)}% of account")
        buy_orders = []
        sell_orders = []
        if self.current_position: # There is an open position already
            logger.info('Already in a trade')
            # Get last filled trade for specified symbol and with a clorID != ''
            last_forder = self.exchange.last_filled_orders_hist_symbol(symbol=self.exchange.symbol, count=20)
            if last_forder:
                self.f_orders = [self.exchange.last_filled_orders_hist_symbol(symbol=self.exchange.symbol, count=20)]
            else:
                self.f_orders = []
            self.update_last_filled(self.f_orders)

            trend = 'None'
            if self.f_orders[-1]['clOrdID'] != '':
                trend = self.f_orders[-1]['clOrdID'].split(' ')[-2] #Last filled trade trend name
            else:
                if self.f_orders[-1]['text'] == 'Funding':
                    lorders = pd.DataFrame(self.exchange.filled_orders_hist_count(4))
                    lorders = self.exchange.filled_orders_hist_count_symbol(symbol=self.exchange.symbol, count=20)
                    filled_order_last = next((forder for forder in lorders if forder['clOrdID'] == ''), False)
                    if filled_order_last:
                        self.f_order = [filled_order_last]
                else:
                    trend = 'None'
                    # Cant find trend for last filled trade
                    logger.warn(f"Cant find trend for previous filled trade, setting TP and SL as for Breakout")

            sl_percen = 0.20  # Setting SL at +-20% of entry
            tp_percen = 0.25  # Setting TP at +-20% of entry

            # Cancel other limit orders from the other side if they are not take profit or SL orders
            self.cancel_limit_orders()
            if trend == 'None':
                # Generating tp and SL orders for open position from an unknown strategy
                buy_orders, sell_orders = self.break_sl_tp_gen(buy_orders, sell_orders)
            elif ('support' not in trend) and ('resistance' not in trend):
                # Generating tp and SL orders for open position from the btw strategy
                buy_orders, sell_orders = self.sl_tp_generator(buy_orders, sell_orders, self.instrument['tickSize'],
                                                               sl_percen, tp_percen, self.trendlines)
                # Maintaining updates on breakout order if the position is from btw strategy
                for i, row in self.trendlines.iterrows():
                    if row['type'] == 'breakout':
                        buy_orders, sell_orders = self.breakout(row, buy_orders, sell_orders)
            else:
                # Generating tp and SL orders for open position from the breakout strategy
                buy_orders, sell_orders = self.break_sl_tp_gen(buy_orders, sell_orders)
        else:
            for i, row in self.trendlines.iterrows():
                if row['type'] != 'breakout':
                    buy_orders, sell_orders = self.btw_res_and_sup(row, buy_orders, sell_orders, self.trendlines)
                else:
                    buy_orders, sell_orders = self.breakout(row, buy_orders, sell_orders)

        return self.converge_orders(buy_orders, sell_orders)

    def break_sl_tp_gen(self, buy_orders, sell_orders):
        entry_price = self.pos_avgentry
        trend_percen = settings.BREAK_TREND_PERCEN
        pos_size = int(np.abs(self.current_position))
        leverage = self.leverage
        ROE_SL = 0.20 # risking 20% of position
        inst = self.exchange.get_instrument()

        mark_price = inst['markPrice']
        #self.last_mark_price = inst['markPrice']
        bid_price = inst['bidPrice']
        ask_price = inst['askPrice']
        trail_percen = 0.003
        for forder in self.f_orders:
            entry_side = forder['side']
            clorid = forder['clOrdID']
            if clorid == '':
                trend_name = 'None'
                id_ident = 'None'
            else:
                trend_name = forder['clOrdID'].split(' ')[-2]
                id_ident = forder['clOrdID'].split(' ')[-1]

            if 'Tp' not in clorid: # We have not taken any profits yet, we can assume pos_size is all we entered with
                #####Ver como manejo esto del porcentaje de la posicion para los TP!!!!!
                self.entry_pos_size = forder['orderQty']
            else:
                self.entry_pos_size = 'None'
        if self.entry_pos_size == 'None':
            # We try to find the initial order position in the order history
            #last_20 = pd.DataFrame(self.exchange.filled_orders_hist_count_symbol(symbol=self.exchange.symbol, count=20))
            last_20 = self.exchange.filled_orders_hist_count_symbol(symbol=self.exchange.symbol, count=20)
            filled_order_last = next((forder for forder in last_20 if 'SBuy' in forder or 'SSell' in forder), False)
            if filled_order_last:
                self.entry_pos_size = filled_order_last['orderQty']
            else:
                logger.warning('Cant found entry position size')
                self.entry_pos_size = pos_size
            #sub_last = last_10[last_10['clOrdID'].str.contains('SBuy')]
            #if len(sub_last) == 0:
            #    sub_last = last_10[last_10['clOrdID'].str.contains('SSell')]
            #if len(sub_last) == 0:
            #    logger.info('Cant found entry position size')
            #    self.entry_pos_size = pos_size
            #else:
            #    self.entry_pos_size = sub_last['orderQty']

        update_trail = True
        if self.current_position < 0:
            tp_side = 'Buy'
            tp_sign = -1
            l_trade = bid_price*(1-trail_percen)
            if mark_price >= self.last_mark_price: # We dont update trail SL
                update_trail = False
        else:
            tp_side = 'Sell'
            tp_sign = 1
            l_trade = ask_price*(1+trail_percen)
            if mark_price <= self.last_mark_price: # We dont update trail SL
                update_trail = False

        # Establishing exits at 10% of profits (divided in 8, 10 and 15) for 75% of position we play with the remaining 25%
        pl_quart = tp_sign*0.05
        pl_half = tp_sign*0.08
        pl_tquart = tp_sign*0.12
        pl_final = tp_sign*0.15
        pl_final_close = tp_sign * 0.18
        pos_size_quart = int(0.15*self.entry_pos_size)
        pos_size_half = int(0.25*self.entry_pos_size)
        pos_size_tquart = int(0.35*self.entry_pos_size)
        pos_size_left = self.entry_pos_size - pos_size_quart - pos_size_half - pos_size_tquart
        sl_order = False
        tp_order_quart = False
        tp_order_half = False
        tp_order_tquart = False
        # Take profit orders
        if ('Tp' not in clorid) and ('qrt' not in clorid):
            # Tp has not been reached we place the order
            exit_pl_quart, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_quart, leverage)
            tp_order_quart = self.prepare_tp_by_price(pos_size_quart, tp_side, exit_pl_quart, 'qrt ' + trend_name)
        if ('Tp' not in clorid) and ('hl' not in clorid):
            # Tp has not been reached we place the order
            exit_pl_half, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_half, leverage)
            tp_order_half = self.prepare_tp_by_price(pos_size_half, tp_side, exit_pl_half, 'hl ' + trend_name)
        if ('Tp' not in clorid) and ('q3rt' not in clorid):
            # Tp has not been reached we place the order
            exit_pl_tquart, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_tquart, leverage)
            tp_order_tquart = self.prepare_tp_by_price(pos_size_tquart, tp_side, exit_pl_tquart, 'q3rt ' + trend_name)

        exit_sl_half, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_half, leverage)
        exit_sl_final, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_final, leverage)
        exit_sl_final_close, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_final_close, leverage)
        # If mark price is close to entry we set an stop loss
        if mark_price <= exit_sl_half and tp_side == 'Sell': #
            logger.info('Setting stop Loss mark < half SL TP')
            sl_price, sl_price2 = poscals.exit_price_ROE(pos_size, entry_price, -tp_sign*ROE_SL, leverage)
            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
        elif mark_price >= exit_sl_half and tp_side == 'Buy': #
            logger.info('Setting stop Loss mark < half SL TP')
            sl_price, sl_price2 = poscals.exit_price_ROE(pos_size, entry_price, -tp_sign*ROE_SL, leverage)
            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
        else:
            # If Tp at 3quart (15%) has been reached we set a trailing stop loss at x% from last bid/ask
            if ('Tp' in clorid) and ('q3rt' in clorid):
                # We just keep up with trail stop up to 20% of wins
                if tp_side == 'Buy' and exit_sl_final_close <= mark_price:
                    sl_order = self.prepare_sl_by_price(pos_size, tp_side, exit_sl_final, trend_name)
                if tp_side == 'Sell' and exit_sl_final_close >= mark_price:
                    sl_order = self.prepare_sl_by_price(pos_size, tp_side, exit_sl_final, trend_name)
                else:
                    # We update_trail SL up to 20% of wins
                    if tp_side == 'Buy':
                        if l_trade <= exit_sl_final_close:
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, exit_sl_final, trend_name)
                        else:
                            if update_trail:
                                sl_order = self.prepare_sl_by_price(pos_size, tp_side, l_trade, trend_name)
                    else:
                        if l_trade >= exit_sl_final_close:
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, exit_sl_final, trend_name)
                        else:
                            if update_trail:
                                sl_order = self.prepare_sl_by_price(pos_size, tp_side, l_trade, trend_name)

            else:
                logger.info('Setting stop Loss mark > 10% TP')
                # If mark price is above 10% profits we set an SL on profit at 5%
                sl_price, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_quart, leverage)
                sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)

        all_orders = [tp_order_quart, tp_order_half, tp_order_tquart, sl_order]
        for tpord in all_orders:
            if tpord:
                if tp_side == 'Buy':
                    buy_orders.append(tpord)
                elif tp_side == 'Sell':
                    sell_orders.append(tpord)
        return buy_orders, sell_orders



    def sl_tp_generator(self, buy_orders, sell_orders, ticksize, sl_percen, tp_percen, trendlines):
        """ Finds wich orders have been filled and creates stop and tp orders """
        ##### !!!!!! Comprobar que el SL esta siempre por encima del liquidation !!!!! ####
        entry_price = self.pos_avgentry
        trend_percen = settings.BTW_TREND_PERCEN
        leverage = self.leverage
        sl_percen = 0.20  # 5% of losses to SL if no other trend defined
        if len(self.f_orders) > 0:
            for forder in self.f_orders:
                entry_side = forder['side']
                # Side to trade
                if entry_side == 'Buy':
                    tp_pre = 'higher'
                    tp_side = 'Sell'
                    trade_type = 'Long'
                    short = False
                    tp_sign = 1
                elif entry_side == 'Sell':
                    tp_pre = 'lower'
                    tp_side = 'Buy'
                    trade_type = 'Short'
                    short = True
                    tp_sign = -1
                else:
                    if self.current_position < 0:
                        entry_side = 'Sell'
                        tp_pre = 'lower'
                        tp_side = 'Buy'
                        trade_type = 'Short'
                        short = True
                        tp_sign = -1
                    else:
                        entry_side = 'Buy'
                        tp_pre = 'higher'
                        tp_side = 'Sell'
                        trade_type = 'Long'
                        short = False
                        tp_sign = 1

                percen_entry = 1 + tp_sign*trend_percen
                percen_exit  = 1 + tp_sign*(-trend_percen-0.01)
                sl_per = tp_sign*(-sl_percen)
                trend_name = forder['clOrdID'].split(' ')[-2]
                trend_id = forder['clOrdID'].split(' ')[-1]
                text_order = entry_side + ' ' + trend_name
                # Locating the symmetric trendline to calculate the exit price
                subf = trendlines.loc[trendlines['trend_name'] == tp_pre + '_' + trend_name.split('_')[-1]]
                print('# Locating the symmetric trendline to calculate the exit price')
                print(subf)
                if len(subf) > 0:
                    logger.info(f"Matching trend found")
                    pos_size = int(np.abs(self.current_position))
                    # Found matching filled order, generating SL and TP orders
                    exit_price = math.toNearest(subf['price_pred'].tolist()[0] * percen_exit, self.instrument['tickSize'])
                    rr, sl_price = poscals.trade_risk_reward(entry_price, exit_price, pos_size, leverage,
                                                             sl_percen=sl_percen)
                    # Making sure SL price is higher/lower than Liquidation price
                    if entry_side == 'Buy':
                        if self.pos_liqprice < sl_price:
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
                        else:
                            sl_price = self.pos_liqprice*1.01
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
                    else:
                        if self.pos_liqprice > sl_price:
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
                        else:
                            sl_price = self.pos_liqprice*0.99
                            sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)

                    trade_info = poscals.profit_loss_cal(pos_size, entry_price, exit_price, leverage,
                                                         funrate=self.fundingrate, short=short)
                    if trade_info['pl_percen'] >= 0.5:
                        # Placing tp orders at different levels
                        pl_quart = tp_sign*trade_info['pl_percen']/4 # Always >= 12.5% of profits
                        pl_half  = tp_sign*trade_info['pl_percen']/2 # Always >= 25.0% of profits

                        exit_pl_quart, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_quart, leverage)
                        exit_pl_half, exit_plwf = poscals.exit_price_pl(pos_size, entry_price, pl_half, leverage)
                        pos_size_half = int(pos_size/2)
                        pos_size_left = pos_size-pos_size_half
                        # Take profit orders
                        tp_order_half = self.prepare_tp_by_price(pos_size_half, tp_side, exit_pl_half,
                                                                 ' h ' + trend_name)
                        tp_order_left = self.prepare_tp_by_price(pos_size_left, tp_side, exit_price,
                                                                 ' l ' + trend_name)
                        logger.info(f"PL {100*trade_info['pl_percen']:1.1f}% Placing tp orders at different levels at {exit_price:1.1f} and {exit_pl_half:1.1f}")

                        if tp_side == 'Buy':
                            buy_orders.extend((tp_order_half, tp_order_left, sl_order))
                        elif tp_side == 'Sell':
                            sell_orders.extend((tp_order_half, tp_order_left, sl_order))
                    else:
                        # Placing only one tp order if possible wins are btw 20% and 50%
                        logger.info(f"PL {trade_info['pl_percen']:1.1f} Placing tp orders at {exit_price:1.1f}")

                        tp_order = self.prepare_tp_by_price(pos_size, tp_side, exit_price,
                                                                 trend_name)

                        if tp_side == 'Buy':
                            buy_orders.extend((tp_order, sl_order))
                        elif tp_side == 'Sell':
                            sell_orders.extend((tp_order, sl_order))

                else:
                    # Cant found corresponding filled order setting SL and TP by hand
                    logger.info(f"Matching trend not found")
                    pos_size = int(np.abs(self.current_position))
                    sl_price, sl_price2 = poscals.exit_price_ROE(pos_size, entry_price, sl_per, leverage)
                    sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, trend_name)
                    exit_price, exitws = poscals.exit_price_pl(pos_size, entry_price, tp_sign*0.1, leverage)
                    tp_order = self.prepare_tp_by_price(pos_size, tp_side, exit_price, trend_name)
                    if tp_side == 'Buy':
                        buy_orders.extend((tp_order, sl_order))
                    elif tp_side == 'Sell':
                        sell_orders.extend((tp_order, sl_order))
        return buy_orders, sell_orders

    def btw_res_and_sup(self, row, buy_orders, sell_orders, trendlines):
        """Strategy to trade between rest and support lines, only post (ie dont post orders that will autoexecute)"""
        # reads the excel with the support and resistance.
        # To trade within a trend and set tp orders, specify common names to trends
        # (i.e. lower_name1 and higher_name1)
        ##### Make a risk/reward analyisis to check if it is worth entering a position
        ##### Select before posting orders (in converge orders) in which trendline we are in, i.e. post only higher buy order and lower sell order
        # And only one open position at a time
        # Entering position +-2% before resistance/support
        trend_percen = settings.BTW_TREND_PERCEN
        # Setting SL at 20% of losses
        sl_percen = 0.20
        # Side to trade
        if 'low' in row['trend_name'] or 'lower' in row['trend_name']:
            side = 'Buy'
            percen_entry = 1 + trend_percen
            tp_pre = 'higher'
            tp_side = 'Sell'
            trade_type = 'Long'
            percen_exit = 1 - trend_percen - 0.01
            short = False
        elif 'up' in row['trend_name'] or 'high' in row['trend_name']:
            side = 'Sell'
            percen_entry = 1 - trend_percen
            tp_pre = 'lower'
            tp_side = 'Buy'
            trade_type = 'Short'
            percen_exit = 1 + trend_percen + 0.01
            short = True

        # Entry price
        entry_price = math.toNearest(row['price_pred'] * percen_entry, self.instrument['tickSize'])
        bid_price = self.exchange.get_instrument()['bidPrice']
        ask_price = self.exchange.get_instrument()['askPrice']
        # CHECK IF ENTRY PRICE is already on the other side
        if (side == 'Sell') and (entry_price < 0.98 * bid_price):
            # If sell order price is lower than 101% of current bid price do nothing
            logger.info(f"BTW: Current sell order at {entry_price:1.1f} is much lower than bid price {bid_price:1.1f}")
            trade_skip = True
        elif (side == 'Buy') and (entry_price > 1.02 * ask_price):
            # If buy order price is higher than 102% of current ask price do nothing
            logger.info(f"BTW: Current buy order at {entry_price:1.1f} is much higher than ask price {ask_price:1.1f}")
            trade_skip = True
        else:
            trade_skip = False
        if trade_skip:
            return buy_orders, sell_orders
        # Locating the symmetric trendline to calculate the exit price
        subf = trendlines.loc[trendlines['trend_name'] == tp_pre + '_' + row['trend_name'].split('_')[-1]]
        if len(subf) > 0:
            exit_price = math.toNearest(subf['price_pred'].tolist()[0] * percen_exit, self.instrument['tickSize'])
        else:
            exit_price = 0
        #Checking if trend is still valid (long lower than short)
        if (side == 'Buy') and (entry_price < exit_price): # We are longing exit price should by higher than entry
            valid_trend = True
        elif (side == 'Sell') and (entry_price > exit_price):  # We are shorting exit price should by lower than entry
            valid_trend = True
        else:
            valid_trend = False

        if valid_trend:
            # Checking if trade is worth entering (i.e. possible rewards vs losses)
            acc_margin_XBT = XBt_to_XBT(self.account_margin)
            pos_size = int(entry_price * acc_margin_XBT * settings.BTW_ORDER_SIZE * settings.LEVERAGE)
            trade_info = poscals.profit_loss_cal(pos_size, entry_price, exit_price, settings.LEVERAGE, funrate=self.fundingrate, short=short)
            rr, sl_price = poscals.trade_risk_reward(entry_price, exit_price, pos_size, settings.LEVERAGE, sl_percen=sl_percen)
            #if trade_info['pl_percen'] < 0.2:
            if rr >= 1.5:# 1.25
                # Trade is not worth entering
                logger.info(f"BTW: Possible PL is {100*trade_info['pl_percen']:1.2f}% and RR={rr:1.2f} Trade not worth entering")
                return buy_orders, sell_orders
            else:
                # Trade order
                logger.info(f"BTW: RR = {rr:1.2f} Entry: {entry_price:1.1f} Exit: {exit_price:1.1f} SL: {sl_price:1.1f} {row['trend_name']}")
                trade_order = self.prepare_order_by_price(pos_size, entry_price, side, row['trend_name'])
                # SL orders # SL are not being posted now, instead when position is opened
                sl_order = self.prepare_sl_by_price(pos_size, tp_side, sl_price, row['trend_name'])
                if side == 'Buy':
                    buy_orders.append(trade_order)
                elif side == 'Sell':
                    sell_orders.append(trade_order)
            return buy_orders, sell_orders
        else:
            logger.info(f"BTW; Trend no longer valid for {trade_type}: Entry {exit_price:1.1f} Exit {entry_price:1.1f}")
            return buy_orders, sell_orders

    def breakout(self, row, buy_orders, sell_orders):
        """Strategy to trade when price break resistance or support"""
        # Orders must be stop orders, they also are going to be market orders
        acc_margin_XBT = XBt_to_XBT(self.account_margin)
        # Side to trade
        if 'support' in row['trend_name'] or 'supp' in row['trend_name']:
            # support line entering short if broken
            side = 'Sell'
            trend_sing = -1
        elif 'resistance' in row['trend_name'] or 'resis' in row['trend_name']:
            # resistance line entering long if broken
            side = 'Buy'
            trend_sing = 1
        percen_entry = 1 + trend_sing*settings.BREAK_TREND_PERCEN
        entry_price = row['price_pred'] * percen_entry
        bid_price = self.exchange.get_instrument()['bidPrice']
        ask_price = self.exchange.get_instrument()['askPrice']
        if (side == 'Sell') and (entry_price > 1.03 * bid_price):
            # If sell order price is lower than 101% of current bid price do nothing
            logger.info(f"Breakout: Current sell order at {entry_price:1.1f} is much higher than bid price {bid_price:1.1f}")
            trade_skip = True
        elif (side == 'Buy') and (entry_price < 0.97 * ask_price):
            # If buy order price is higher than 102% of current ask price do nothing
            logger.info(f"Breakout: Current buy order at {entry_price:1.1f} is much lower than ask price {ask_price:1.1f}")
            trade_skip = True
        else:
            trade_skip = False
        if trade_skip:
            return buy_orders, sell_orders
        pos_size = int(entry_price * acc_margin_XBT * settings.BREAK_ORDER_SIZE * settings.LEVERAGE)
        trade_order = self.prepare_slbuy_by_price(pos_size, side, entry_price, row['trend_name'])
        if side == 'Buy':
            buy_orders.append(trade_order)
        elif side == 'Sell':
            sell_orders.append(trade_order)
        return buy_orders, sell_orders

    #def prepare_order(self, index):
    #    """Create an order object."""
    #    if settings.RANDOM_ORDER_SIZE is True:
    #        quantity = random.randint(settings.MIN_ORDER_SIZE, settings.MAX_ORDER_SIZE)
    #    else:
    #        quantity = settings.ORDER_START_SIZE + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)
    #    price = self.get_price_offset(index)
    #    return {'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}
        
    def prepare_order_me(self, price, side, text):
        """Create an order object."""
        quantity = int(price*XBt_to_XBT(self.exchange.get_margin()['amount'])*settings.ORDER_SIZE)

        return {'price': price, 'orderQty': quantity, 'side': side, 'clOrdID': text}

    def prepare_order_btw(self, price, side, text):
        """Create an order object for the trade btw resistances and supports strategy"""
        # order size = account balnce * X%
        quantity = int(price*XBt_to_XBT(self.exchange.get_margin()['amount'])*settings.BTW_ORDER_SIZE)
        return {'ordType': 'Limit', 'price': price, 'orderQty': quantity, 'side': side, 'clOrdID': side + ' ' + text}

    def prepare_order_by_price(self, quantity, price, side, text):
        """Create an order object given a price and quantity"""
        return {'ordType': 'Limit', 'price': price, 'orderQty': quantity, 'side': side, 'clOrdID': side + ' ' + text}

    def prepare_sl_by_price(self, quantity, side, slprice, text):
        # Setting stop loss to open position
        sl_price = math.toNearest(slprice, self.instrument['tickSize'])
        # Close: Close implies ReduceOnly. A Close order will cancel other active limit orders with the same side and symbol if the open quantity exceeds the current position.
        return {'ordType': 'Stop', 'stopPx': sl_price, 'orderQty': quantity, 'side': side, 'execInst': 'ReduceOnly',
                'clOrdID': 'SL ' + text}

        # return {'ordType': 'Stop', 'stopPx': sl_price, 'orderQty': quantity, 'side': side, 'execInst': 'ReduceOnly', 'text': 'Stop loss ' + text}

    def prepare_slbuy_by_price(self, quantity, side, slprice, text):
        # Setting stop buy or sell for entering a position
        sl_price = math.toNearest(slprice, self.instrument['tickSize'])
        return {'ordType': 'Stop', 'stopPx': sl_price, 'orderQty': quantity, 'side': side,
                'clOrdID': 'S'+side + ' ' + text}

    def prepare_tp_by_price(self, quantity, side, tpprice, text):
        # Setting take profit for an existing position
        tp_price = math.toNearest(tpprice, self.instrument['tickSize'])
        return {'ordType': 'Limit', 'price': tp_price, 'orderQty': quantity, 'side': side, 'execInst': 'ReduceOnly',
                'clOrdID': 'Tp ' + text}

    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.exchange.get_instrument()['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.exchange.get_orders(self.exchange.symbol) # Existing orders on bmex

        self.current_position = self.exchange.get_position()['currentQty'] # The current position amount in contracts. (contracts)
        self.pos_cost = self.exchange.get_position()['grossOpenCost'] # grossOpenCost: The absolute of your open orders for this symbol. (XBt)
        self.pos_margin = self.exchange.get_position()['posMargin'] # Position Margin (XBt)
        self.account_margin = self.exchange.get_margin()['amount']  # Total Account Marg in (XBt)
        self.available_margin = self.exchange.get_margin()['availableMargin'] # Available Margin Balance (XBt)

        new_orders = buy_orders + sell_orders

        exist_df = pd.DataFrame(existing_orders)
        if len(existing_orders)>0:
            exist_text = exist_df['clOrdID'].to_list()
            # If there are open orders we check if some needs to be amended
            for desired_order in new_orders:
                if desired_order['ordType'] == 'Stop':
                    des_price_text = 'stopPx'
                else:
                    des_price_text = 'price'
                # Locating if order text is already inside an existing order
                #if desired_order['origClOrdID'] in exist_text:
                if b_any(desired_order['clOrdID'] in x for x in exist_text):
                    sub_exist_df = exist_df.loc[exist_df['clOrdID'].str.contains(desired_order['clOrdID'])]
                    sub_exist_df.reset_index(inplace=True)

                    if sub_exist_df is not None:# or (len(sub_exist_df) == 0):
                        # Found an existing order amending only if price or quanity has changed
                        if (desired_order[des_price_text] != sub_exist_df[des_price_text][0]) or (desired_order['orderQty'] != sub_exist_df['leavesQty'][0]):
                            desired_order['orderID'] = sub_exist_df['orderID'][0]
                            desired_order['clOrdID'] = sub_exist_df['clOrdID'][0]
                            to_amend.append(
                                {'orderID': sub_exist_df['orderID'][0], 'ordType': desired_order['ordType'], 'orderQty': desired_order['orderQty'],
                                 des_price_text: desired_order[des_price_text], 'side': desired_order['side']})
                else:
                    # Order not found in existing, creating
                    to_create.append(desired_order)
        else:
            # No existing orders all are to create
            to_create.extend(new_orders)

        if len(to_amend) > 0:
            print('\n\n +++++++++++ Amending orders +++++++++')
            print(to_amend)
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                #reference_order = [o for o in existing_orders if o['clOrdID'] == amended_order['clOrdID']][0]
                # Checking order type
                if reference_order['ordType'] == 'Stop':
                    price_text = 'stopPx'
                else:
                    price_text = 'price'
                if amended_order['ordType'] == 'Stop':
                    des_price_text = 'stopPx'
                else:
                    des_price_text = 'price'

                logger.info("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order[price_text],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order[des_price_text],
                    tickLog, (amended_order[des_price_text] - reference_order[price_text])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.exchange.amend_bulk_orders(to_amend)
            except requests.exceptions.HTTPError as e:
                errorObj = e.response.json()
                if errorObj['error']['message'] == 'Invalid ordStatus':
                    logger.warn("Amending failed. Waiting for order data to converge and retrying.")
                    # It's possible, given latency over the internet, that an order fills or cancels
                    # while the bot is trying to amend it. If that happens, the bot halts the current
                    # tick, waits 500ms, and starts another. This ensures the new order status
                    # has been reflected in the data from the websocket.
                    sleep(1.0)
                    print('\n\n\n+++++++ Error order to Amend ++++++++')
                    print(to_amend)
                    print('\n\n\n+++++++ Existing orders +++++++++++')
                    print(self.exchange.get_orders(self.exchange.symbol))
                    print('+++++++++++++++++++++++++++\n\n')
                    return self.place_orders()
                else:
                    logger.error("Unknown error on amend: %s. Exiting" % errorObj)
                    sys.exit(1)


        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            print('\n\n +++++++++++ Creating orders +++++++++')
            print(to_create)
            for order in reversed(to_create):
                if 'price' in order:
                    order_price = order['price']
                elif 'stopPx' in order:
                    order_price = order['stopPx']
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order_price))
            self.exchange.create_bulk_orders(to_create)


        # Could happen if we exceed a delta limit
        cancel_to = []
        if len(to_cancel) > 0:
            for order in reversed(to_cancel):
                if (order['ordType'] != 'Stop') or  ('Tp' not in order['clOrdID']):
                    # Not canceling stop orders nor take profit orders
                    cancel_to.append(order)
        if len(cancel_to) > 0:
            logger.info("Canceling %d orders:" % (len(cancel_to)))
            for order in cancel_to:
                if order['price']:
                    order_price = order['price']
                elif order['stopPx']:
                    order_price = order['stopPx']
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order_price))
            self.exchange.cancel_bulk_orders(cancel_to)


    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position >= settings.MAX_POSITION

    ###
    # Sanity
    ##

    def sanity_check(self):
        """Perform checks before placing orders."""

        # Check if OB is empty - if so, can't quote.
        self.exchange.check_if_orderbook_empty()

        # Ensure market is still open.
        self.exchange.check_market_open()

        # Get ticker, which sets price offsets and prints some debugging info.
        ticker = self.get_ticker()

        # Sanity check:
        #if self.get_price_offset(-1) >= ticker["sell"] or self.get_price_offset(1) <= ticker["buy"]:
        #    logger.error("Buy: %s, Sell: %s" % (self.start_position_buy, self.start_position_sell))
        #    logger.error("First buy position: %s\nBitMEX Best Ask: %s\nFirst sell position: %s\nBitMEX Best Bid: %s" %
        #                 (self.get_price_offset(-1), ticker["sell"], self.get_price_offset(1), ticker["buy"]))
        #    logger.error("Sanity check failed, exchange data is inconsistent")
        #    self.exit()

        # Messaging if the position limits are reached
        if self.long_position_limit_exceeded():
            logger.info("Long delta limit exceeded")
            logger.info("Current Position: %.f, Maximum Position: %.f" %
                        (self.exchange.get_delta(), settings.MAX_POSITION))

        if self.short_position_limit_exceeded():
            logger.info("Short delta limit exceeded")
            logger.info("Current Position: %.f, Minimum Position: %.f" %
                        (self.exchange.get_delta(), settings.MIN_POSITION))

    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def check_connection(self):
        """Ensure the WS connections are still open."""
        return self.exchange.is_open()

    def exit(self):
        logger.info("Shutting down. All open limit orders will be cancelled.")
        for i in range(0, 5): # 5 tries to restart Tom
            try:
                self.exchange.cancel_all_orders()
                self.exchange.bitmex.exit()
                ### Trying to relaunch on error
                #print('\n\n\n++++++ Trying relaunch Tom Bot +++++++++')
                #sleep(2.0)
                #run()
            except KeyboardInterrupt:
                self.exchange.cancel_all_orders()
                self.exchange.bitmex.exit()
                telegram_bot.telegram_bot_sendtext(f"Tom bot has been manually closed.")
                sys.exit()
            except errors.AuthenticationError as e:
                logger.info("Was not authenticated; could not cancel orders.")
                continue
            except Exception as e:
                logger.info("Unable to cancel orders: %s" % e)
                continue
            else:
                logger.info("Unable to restart Tom, shutting down")
                # Send an email and a telegram message on this
                break


            #sys.exit()

    def update_wallet(self):
        # Updating filled orders
        path_trend = '/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/'
        margen = self.exchange.get_margin()
        dict = [{'acc_mar': margen['amount'], 'ava_mar': margen['availableMargin'],
                'date': datetime.now().strftime("%Y/%m/%d %H:%M:%S")}]
        wallet_df_new = pd.DataFrame(dict)
        if not os.path.isfile(path_trend + 'historic/acc_balance.csv'):
            # Building historical balance if first time
            wallet_df_new.to_csv(path_trend + 'historic/acc_balance.csv', sep='\t', index=False)
            return wallet_df_new
        else:
            # Reading previous historical wallet balance
            wallet_df = pd.read_csv(path_trend + 'historic/acc_balance.csv', sep='\t')
            # Merging new acc balance (drop if they have the same date and time)
            wallet_df = pd.concat([wallet_df, wallet_df_new], ignore_index=True, sort=False).drop_duplicates(
                ['date'], keep='first')
            wallet_df.to_csv(path_trend + 'historic/acc_balance.csv', sep='\t', index=False)
            return wallet_df

    def run_loop(self):
        self.wakeup_time = 0
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()

            self.check_file_change()
            self.last_mark_price = self.exchange.get_instrument()['markPrice']
            sleep(settings.LOOP_INTERVAL)

            logger.info(f'Wake up time\t{self.wakeup_time}')
            if (self.wakeup_time % 300 == 0) and (self.wakeup_time != 0): # Updating historical every 5min
                tfs = ['5m', '1h', '1d']
                logger.info('Updating historical chart prices')
                for tf in tfs:
                    #get_bitmex_data.get_all_bitmex('/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/historic/','XBTUSD', tf, save=True)
                    get_bitmex_data.get_all_bitmex('/anaconda2/envs/bincrypy/lib/python3.7/site-packages/market_maker/historic/', self.exchange.symbol, tf, save=True)
            if (self.wakeup_time % 1800 == 0) and (self.wakeup_time != 0):  # Updating account balance every 30min
                self.update_wallet()

            # This will restart on very short downtime, but if it's longer,
            # the MM will crash entirely as it is unable to connect to the WS on boot.
            if not self.check_connection():
                logger.error("Realtime data connection unexpectedly closed, restarting.")
                self.restart()

            self.sanity_check()  # Ensures health of mm - several cut-out points here
            lm_price = self.print_status()  # Print skew, delta, etc
            if (self.wakeup_time % 30 == 0): # Last marke price from 30seg ago
                self.last_mark_price = lm_price
            self.place_orders(self.wakeup_time)  # Creates desired orders and converges to existing orders
            self.wakeup_time += settings.LOOP_INTERVAL

    def restart(self):
        logger.info("Restarting Tom...")
        os.execv(sys.executable, [sys.executable] + sys.argv)

#
# Helpers
#
def run_once_plotter(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
        wrapper.has_run = False
        return wrapper

def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


def cost(instrument, quantity, price):
    mult = instrument["multiplier"]
    P = mult * price if mult >= 0 else mult / price
    return abs(quantity * P)


def margin(instrument, quantity, price):
    return cost(instrument, quantity, price) * instrument["initMargin"]


def run():
    #logger.info('My BitMEX bot')
    logger.info('BitMEX Tom Bot Version: %s\n' % constants.VERSION)
    om = OrderManager()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        om.run_loop()
    except (KeyboardInterrupt, SystemExit):
        telegram_bot.telegram_bot_sendtext(f"Tom bot has been closed.")
        sys.exit()
