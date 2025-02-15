from os.path import join
import logging

########################################################################################################################
# Connection/Auth
########################################################################################################################

# API URL.
BASE_URL = "https://testnet.bitmex.com/api/v1/"
# BASE_URL = "https://www.bitmex.com/api/v1/" # Once you're ready, uncomment this.

# The BitMEX API requires permanent API keys. Go to https://testnet.bitmex.com/app/apiKeys to fill these out.
API_KEY = ""
API_SECRET = ""


########################################################################################################################
# Target
########################################################################################################################

# Instrument to trade on BitMEX.
SYMBOL = "XBTUSD"


########################################################################################################################
# Order Size & Spread
########################################################################################################################

# How many pairs of buy/sell orders to keep open
ORDER_PAIRS = 6

# ORDER_START_SIZE will be the number of contracts submitted on level 1
# Number of contracts from level 1 to ORDER_PAIRS - 1 will follow the function
# [ORDER_START_SIZE + ORDER_STEP_SIZE (Level -1)]
ORDER_START_SIZE = 100
ORDER_STEP_SIZE = 100

# Distance between successive orders, as a percentage (example: 0.005 for 0.5%)
INTERVAL = 0.005

# Minimum spread to maintain, in percent, between asks & bids
MIN_SPREAD = 0.01

# If True, market-maker will place orders just inside the existing spread and work the interval % outwards,
# rather than starting in the middle and killing potentially profitable spreads.
MAINTAIN_SPREADS = True

# This number defines far much the price of an existing order can be from a desired order before it is amended.
# This is useful for avoiding unnecessary calls and maintaining your ratelimits.
#
# Further information:
# Each order is designed to be (INTERVAL*n)% away from the spread.
# If the spread changes and the order has moved outside its bound defined as
# abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL)
# it will be resubmitted.
#
# 0.01 == 1%
RELIST_INTERVAL = 0.01


########################################################################################################################
# Trading Behavior
########################################################################################################################

# Position limits - set to True to activate. Values are in contracts.
# If you exceed a position limit, the bot will log and stop quoting that side.
CHECK_POSITION_LIMITS = False
MIN_POSITION = -10000
MAX_POSITION = 10000

# If True, will only send orders that rest in the book (ExecInst: ParticipateDoNotInitiate).
# Use to guarantee a maker rebate.
# However -- orders that would have matched immediately will instead cancel, and you may end up with
# unexpected delta. Be careful.
POST_ONLY = False

# Max price allowed below trendline to make order
TREND_PRICE_PERCEN = 0.0005  # If price is >+- 0.0005 when posting order, it will not be posted  (to avoid trend changes)
# Order sizes
BTW_TREND_PERCEN = 0.01 # Enter/exit % above/below support/resistance for resistance and support strategy
BTW_ORDER_SIZE = 0.10 # % of portfolio to trade between resistance and support
BREAK_ORDER_SIZE = 0.20 # % of portfolio to trade between resistance and support
BREAK_TREND_PERCEN = 0.03 # Enter/exit % above/below support/resistance for breakout strategy
BREAK_ORDER_SIZE = 0.20 # % of portfolio to trade in a breakout
LEVERAGE = 20 # Leverage
TRADING_LIMIT = 0.60 # max % of portfolio to be alocated (should be like a 2nd barrier now)
########################################################################################################################
# Misc Behavior, Technicals
########################################################################################################################

# If true, don't set up any orders, just say what we would do
# DRY_RUN = True
DRY_RUN = False

# How often to re-check and replace orders.
# Generally, it's safe to make this short because we're fetching from websockets. But if too many
# order amend/replaces are done, you may hit a ratelimit. If so, email BitMEX if you feel you need a higher limit.
LOOP_INTERVAL = 10#5

# Wait times between orders / errors
API_REST_INTERVAL = 1
API_ERROR_INTERVAL = 10
TIMEOUT = 12

# If we're doing a dry run, use these numbers for BTC balances
DRY_BTC = 50

# Available levels: logging.(DEBUG|INFO|WARN|ERROR)
LOG_LEVEL = logging.INFO

# To uniquely identify orders placed by this bot, the bot sends a ClOrdID (Client order ID) that is attached
# to each order so its source can be identified. This keeps the market maker from cancelling orders that are
# manually placed, or orders placed by another bot.
#
# If you are running multiple bots on the same symbol, give them unique ORDERID_PREFIXes - otherwise they will
# cancel each others' orders.
# Max length is 13 characters.
ORDERID_PREFIX = "mm_bitmex_"

# If any of these files (and this file) changes, reload the bot.
WATCHED_FILES = [join('market_maker', 'market_maker.py'), join('market_maker', 'bitmex.py'), 'settings.py']


########################################################################################################################
# BitMEX Portfolio
########################################################################################################################

# Specify the contracts that you hold. These will be used in portfolio calculations.
CONTRACTS = ['XBTUSD']
