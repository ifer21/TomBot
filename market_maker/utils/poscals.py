import numpy as np
from market_maker.utils import math, constants
import logging



def profit_loss_cal(pos_size, entry_price, exit_price, leverage, funrate=0, short=False):
    logger = logging.getLogger('root')
    margin_XBT_nofees = pos_size / leverage / entry_price
    entry_value_XBT = pos_size / entry_price
    maker_fee = 0.025 / 100  # fee for a maker order
    taker_fee = 0.075 / 100  # fee for a taker order
    entry_fees_xbt = entry_value_XBT * maker_fee
    exit_value_XBT = pos_size / exit_price
    exit_fees_xbt = exit_value_XBT * taker_fee
    profit_loss_XBT = entry_value_XBT - exit_value_XBT
    ROE_percen = profit_loss_XBT / margin_XBT_nofees
    profit_loss_percen = (exit_price - entry_price) / exit_price
    mm = maint_margin(pos_size, entry_price, margin_XBT_nofees, funrate)
    margin_XBT_wfees = margin_XBT_nofees + mm + entry_fees_xbt + exit_fees_xbt
    ROE_percen_wfees = profit_loss_XBT / margin_XBT_wfees
    if short:
        profit_loss_XBT = -profit_loss_XBT
        profit_loss_percen = -profit_loss_percen
        ROE_percen = -ROE_percen

    #logger.info(f"PL= {profit_loss_XBT:1.4f} ({100 * profit_loss_percen:1.2f}%) ROE={100 * ROE_percen:1.2f}%")
    return {'margin_nofees': margin_XBT_nofees, 'margin_wfees': margin_XBT_wfees, 'entry_value_XBT': entry_value_XBT, 'exit_value_XBT': exit_value_XBT,
            'pl_XBT': profit_loss_XBT, 'pl_percen': profit_loss_percen, 'ROE': ROE_percen}


def maint_margin(pos_size, entry_price, margin_XBT_nofees, funrate):
    taker_fee = 0.075 / 100  # fee for a taker order
    entry_value_XBT = pos_size / entry_price
    br_price = pos_size / (pos_size / entry_price + margin_XBT_nofees)
    br_value = pos_size / br_price
    maint_margin = entry_value_XBT * 0.5 / 100 + (taker_fee * br_value) + (funrate * br_value)
    return maint_margin


def liq_price(avail_margin_XBT, entry_price, leverage, short=False):
    # No funciona muy bien para shorts no se que falta
    taker_fee = 0.075 / 100  # fee for a taker order
    margin_XBT_nofees = avail_margin_XBT
    pos_size = entry_price*leverage*margin_XBT_nofees
    entry_value_XBT = pos_size / entry_price
    mm = entry_value_XBT * 0.5 / 100
    if short:
        pos_sign = -1
        correction = - 0.25 / 100
    else:
        pos_sign = 1
        correction = 0
    l_price = pos_size / (pos_size / entry_price + pos_sign*(margin_XBT_nofees - mm ))
    exit_value_XBT = pos_size / l_price
    exit_fees_xbt = exit_value_XBT * taker_fee
    l_price_v2 = pos_size / (pos_size / entry_price + pos_sign*(margin_XBT_nofees - mm + pos_sign*exit_fees_xbt))
    #dist1 = 100*np.abs(l_price-liq_price_bm)/entry_price
    #dist2 = 100*np.abs(l_price_v2 - liq_price_bm) / entry_price
    #print(f"liq_bm {liq_price_bm:1.1f}  liq {l_price:1.1f} ({dist1:1.4f}%) liq2 {l_price_v2+correction*l_price_v2:1.1f} ({dist2:1.4f}%)")
    return l_price_v2+correction*l_price_v2


def exit_price_ROE(pos_size, entry_price, ROE_percen, leverage):
    # For short ROE_percen < 0
    margin_XBT_nofees = pos_size / leverage / entry_price
    maker_fee = 0.025 / 100  # fee for a maker order
    exit_price = entry_price / (1 - ROE_percen / leverage)
    exit_value_XBT = pos_size / exit_price
    maint_margin_limit = (maker_fee * exit_value_XBT) #+ (funrate * exit_value_XBT)
    maint_margin_percen = maint_margin_limit/margin_XBT_nofees
    exit_price_wfees = entry_price / (1-(ROE_percen+maint_margin_percen)/leverage)
    return exit_price, exit_price_wfees

def exit_price_pl(pos_size, entry_price, profit_loss_percen, leverage):
    # For short profit_loss_percen < 0
    margin_XBT_nofees = pos_size / leverage / entry_price
    maker_fee = 0.025 / 100  # fee for a maker order
    ROE_percen = profit_loss_percen * leverage
    exit_price = entry_price / (1 - ROE_percen / leverage)
    exit_value_XBT = pos_size / exit_price
    maint_margin_limit = (maker_fee * exit_value_XBT) #+ (funrate * exit_value_XBT)
    maint_margin_percen = maint_margin_limit/margin_XBT_nofees
    exit_price_wfees = entry_price / (1-(ROE_percen+maint_margin_percen)/leverage)
    return exit_price, exit_price_wfees

def ROE_Liq(entry_price, leverage, short=False):
    """ Calculates de ROE for liquidation given a leverage"""
    # At 100x leverage liquidation occurs at ROE = -65% (long)
    # At 100x leverage liquidation occurs at ROE = +24.5% (short)
    roe1 = 100-65
    ROE_liq = 100-roe1/(100/leverage)
    if short:
        roe1 = 100 - 24.5
        ROE_liq = -(100 - roe1 / (100 / leverage))
    liq1, liq2 = exit_price_ROE(20000, entry_price, -ROE_liq/100, leverage)
    return ROE_liq/100, liq2

def trade_risk_reward(entry_price, exit_price, pos_size, leverage, sl_percen=0.2):
    # Risk Reward < 1 : Potential wins are greatter than potential losses
    # Risk Reward > 1 : Potential losses are greatter than potential wins
    if entry_price < exit_price:
        # Long
        short = False
        ROE_SL = - sl_percen
    else:
        # Short
        short = True
        ROE_SL = sl_percen
    sl_price, sl_price2 = exit_price_ROE(pos_size, entry_price, ROE_SL, leverage)
    diff_entry_exit = np.abs(entry_price - exit_price)  # Reward
    diff_entry_loss = np.abs(entry_price - sl_price)    # Risk
    r_reward = np.abs((entry_price - sl_price) / (exit_price - entry_price))
    roel, liqp = ROE_Liq(entry_price, leverage, short)
    #print(f"Entry: {entry_price:1.1f} Exit: {exit_price:1.1f} SL {sl_price:1.1f} RR: {r_reward:1.2f} Liq {liqp:1.1f}")
    return r_reward, sl_price

def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT
