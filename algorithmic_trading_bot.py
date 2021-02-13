from __future__ import print_function
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import pandas as pd
import time
import pytz
import schedule
import talib as ta
import strategy
import constants
import sys

def connect(account):
    account = int(account)
    mt5.initialize()
    authorized=mt5.login(account)

    if authorized:
        print("Connected: Connecting to MT5 Client")
    else:
        print("Failed to connect at account #{}, error code: {}"
              .format(account, mt5.last_error()))

def open_position(pair, order_type, size, tp_distance=None, stop_distance=None):
    symbol_info = mt5.symbol_info(pair)
    if symbol_info is None:
        print(pair, "not found")
        return

    if not symbol_info.visible:
        print(pair, "is not visible, trying to switch on")
        if not mt5.symbol_select(pair, True):
            print("symbol_select({}}) failed, exit",pair)
            return
    print(pair, "found!")

    point = symbol_info.point

    if(order_type == "BUY"):
        order = mt5.ORDER_TYPE_BUY
        price = mt5.symbol_info_tick(pair).ask
        if(stop_distance):
            sl = price - (stop_distance * point)
        if(tp_distance):
            tp = price + (tp_distance * point)

    if(order_type == "SELL"):
        order = mt5.ORDER_TYPE_SELL
        price = mt5.symbol_info_tick(pair).bid
        if(stopDistance):
            sl = price + (stop_distance * point)
        if(tpDistance):
            tp = price - (tp_distance * point)

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pair,
        "volume": float(size),
        "type": order,
        "price": price,
        "sl": sl,
        "tp": tp,
        "magic": 234000,
        "comment": "",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print("Failed to send order :(")
    else:
        print ("Order successfully placed!")

def positions_get(symbol=None):
    if(symbol is None):
        res = mt5.positions_get()
    else:
        res = mt5.positions_get(symbol=symbol)

    if(res is not None and res != ()):
        df = pd.DataFrame(list(res),columns=res[0]._asdict().keys())
        df['time'] = pd.to_datetime(df['time'], unit='s')
        return df

    return pd.DataFrame()

def close_position(deal_id):
    open_positions = positions_get()
    open_positions = open_positions[open_positions['ticket'] == deal_id]
    order_type  = open_positions["type"][0]
    symbol = open_positions['symbol'][0]
    volume = open_positions['volume'][0]

    if(order_type == mt5.ORDER_TYPE_BUY):
        order_type = mt5.ORDER_TYPE_SELL
        price = mt5.symbol_info_tick(symbol).bid
    else:
        order_type = mt5.ORDER_TYPE_BUY
        price = mt5.symbol_info_tick(symbol).ask

    close_request={
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type,
        "position": deal_id,
        "price": price,
        "magic": 234000,
        "comment": "Close trade",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(close_request)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print("Failed to close order :(")
    else:
        print ("Order successfully closed!")

def close_positon_by_symbol(symbol):
    open_positions = positions_get(symbol)
    open_positions['ticket'].apply(lambda x: close_position(x))

def get_data(time_frame, strategy):
    pairs = strategy['pairs']
    pair_data = dict()
    for pair in pairs:
        utc_from = datetime(2021, 1, 1, tzinfo=pytz.timezone('Europe/Athens'))
        date_to = datetime.now().astimezone(pytz.timezone('Europe/Athens'))
        date_to = datetime(date_to.year, date_to.month, date_to.day, hour=date_to.hour, minute=date_to.minute)
        rates = mt5.copy_rates_range(pair, time_frame, utc_from, date_to)
        rates_frame = pd.DataFrame(rates)
        rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
        rates_frame.drop(rates_frame.tail(1).index, inplace = True)
        pair_data[pair] = rates_frame
        print(pair_data[pair])
    return pair_data
x_time = ":32"
def live_trading(strategy):
    schedule.every().hour.at(":00").do(run_trader, mt5.TIMEFRAME_M15, strategy)
    schedule.every().hour.at(":15").do(run_trader, mt5.TIMEFRAME_M15, strategy)
    schedule.every().hour.at(":30").do(run_trader, mt5.TIMEFRAME_M15, strategy)
    schedule.every().hour.at(":45").do(run_trader, mt5.TIMEFRAME_M15, strategy)
    schedule.every().hour.at(x_time).do(run_trader, mt5.TIMEFRAME_M15, strategy)

    while True:
        schedule.run_pending()
        time.sleep(1)

def check_trades(time_frame, pair_data, strategy):
    moving_averages = strategy['movingAverages']
    for pair, data in pair_data.items():
        for m in moving_averages:
            ma_func = constants.movingAveragesFunctions[m]
            val = moving_averages [m]['val']
            data[m] = ma_func(data['close'], val)
        # data['SMA'] = ta.SMA(data['close'], 10)
        # data['EMA'] = ta.EMA(data['close'], 50)
        last_row = data.tail(1)
        open_positions = positions_get()
        current_dt = datetime.now().astimezone(pytz.timezone('Europe/Athens'))
        for index, position in open_positions.iterrows():
            # Check to see if the trade has exceeded the time limit
            trade_open_dt = position['time'].replace(tzinfo = pytz.timezone('Europe/Athens'))
            deal_id = position['ticket']
            if(current_dt - trade_open_dt >= timedelta(hours = 2)):
                close_position(deal_id)
        for index, last in last_row.iterrows():
            #Exit strategy
            if(last['close'] < last['EMA'] and last['close'] > last['SMA']):
                close_positon_by_symbol(pair)

            #Entry strategy
            if(last['close'] > last['EMA'] and last['close'] < last['SMA']):
                lot_size = calc_position_size(pair, strategy)
                open_position(pair, "BUY", lot_size, float (strategy['takeProfit']), float(strategy['stopLoss']))
                # open_position(pair, "BUY", 1, float (strategy['takeProfit']), float(strategy['stopLoss']))
                # open_position(pair, "BUY", 1, 300, 100)

def run_trader(time_frame):
    print("Running trader at", datetime.now())
    connect(41087787)
    pair_data = get_data(time_frame)
    check_trades(time_frame, pair_data)

def calc_position_size(symbol, strategy):
    print("Calculating position size for: ", symbol)
    account = mt5.account_info()
    balance = float(account.balance)
    pip_value = constants.getPipValue(symbol, strategy['account_currency'])
    lot_size = (float(balance) * (float(strategy["risk"])/100)) / (pip_value * strategy["stopLoss"])
    lot_size = round(lot_size, 2)
    return lot_size

if __name__ == '__main__':
    print("Trying....")
    current_strategy = "myStrategy" # sys.argv[1]
    print("Trading bot started with strategy: ", current_strategy)
    current_strategy = strategy.load_strategy(current_strategy)
    live_trading(current_strategy)

# connect(41087787)
# open_position("EURUSD", "BUY", 1, 800, 400)
# live_trading()
# close_positon_by_symbol("EURUSD")
# print(sys.argv, len(sys.argv))


print("Done. ")
