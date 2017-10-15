import sys
sys.path.append('/home/market/code/market_data/')
import pandas as pd
import numpy as np
from common.data_source_factory import DataSourceFactory

def combine_trade_tick(trade, tick):

    # combine the trade_table and tick_table together
    trade['in_bar'] = trade.index
    trade['acc_buy_volume'] = pd.Series(np.zeros(len(trade))).values
    trade['acc_buy_value'] = pd.Series(np.zeros(len(trade))).values
    trade['acc_sell_volume'] = pd.Series(np.zeros(len(trade))).values
    trade['acc_sell_value'] = pd.Series(np.zeros(len(trade))).values

    tick['open'] = pd.Series(np.zeros(len(tick))).values
    tick['high'] = pd.Series(np.zeros(len(tick))).values
    tick['low'] = pd.Series(np.zeros(len(tick))).values
    tick['close'] = pd.Series(np.zeros(len(tick))).values
    tick['volume'] = pd.Series(np.zeros(len(tick))).values
    tick['turnover'] = pd.Series(np.zeros(len(tick))).values
    tick['vwap'] = pd.Series(np.zeros(len(tick))).values
    tick['c_vol_int'] = pd.Series(np.zeros(len(tick))).values

    # convert c_vol into float
    for k in range(len(tick)):
        tick['c_vol_int'].iloc[k] = float(tick['c_vol'].iloc[k])

    # calculate the starting point in trade_table
    j = 0  # the index of the trade
    acc_buy_volume = 0
    acc_buy_value = 0
    acc_sell_volume = 0
    acc_sell_value = 0

    for i in range(len(tick)):
        c_vol = tick['c_vol_int'].iloc[i]
        acc_volume = 0
        acc_turnover = 0.0
        distance = 100000000  # volume distance
        index_start = j
        if index_start >= len(trade):
            print 'hit the end of trade_table with incomplete tick_table'
            break

        while True:

            if j < len(trade):
                new_distance = abs(int(c_vol * 100) - acc_volume - trade['amount'].iloc[j])
                if new_distance < distance:
                    distance = new_distance
                else:
                    break
            else:
                break

            # update trade table, the amount is float64
            if trade['flag'].iloc[j] == 'B':
                acc_buy_volume += trade['amount'].iloc[j]
                acc_buy_value += trade['amount'].iloc[j] * trade['price'].iloc[j]
            elif trade['flag'].iloc[j] == 'S':
                acc_sell_volume += trade['amount'].iloc[j]
                acc_sell_value += trade['amount'].iloc[j] * trade['price'].iloc[j]

            trade['in_bar'].iloc[j] = tick.index[i]
            trade['acc_buy_volume'].iloc[j] = acc_buy_volume
            trade['acc_buy_value'].iloc[j] = acc_buy_value
            trade['acc_sell_volume'].iloc[j] = acc_sell_volume
            trade['acc_sell_value'].iloc[j] = acc_sell_value

            acc_volume += trade['amount'].iloc[j]
            acc_turnover += trade['price'].iloc[j] * trade['amount'].iloc[j]

            j += 1

        index_end = j - 1
        # print '%s, %s, %s' % (tick.iloc[i].name, index_start, index_end)

        # now this tick contains trade[index_start, index_end], both included
        tick['open'].iloc[i] = trade['price'].iloc[index_start]
        tick['high'].iloc[i] = trade['price'].iloc[index_start:index_end+1].max()
        tick['low'].iloc[i] = trade['price'].iloc[index_start:index_end+1].min()
        tick['close'].iloc[i] = trade['price'].iloc[index_end]
        tick['volume'].iloc[i] = acc_volume
        tick['turnover'].iloc[i] = acc_turnover
        if acc_volume > 0:
            tick['vwap'].iloc[i] = acc_turnover / acc_volume

    return trade, tick

if __name__ == '__main__':
    market = DataSourceFactory.get_data_source()
    market.create_collection('TICK_AFTER')
    market.create_collection('TRADE_AFTER')

    instrument = ['600348.SH', '000552.SZ']
    for instrument_ in instrument:
        tick_set = set()
        tick = market.get_tick(instrument_, '2017-01-01 00:00:00', '2017-10-15 20:30:00')
        for index in tick.index:
            tick_set.add(index.strftime('%Y-%m-%d'))
        for d in tick_set:
            tick_ = market.get_tick(instrument_, d + ' 09:00:00', d + ' 16:00:00')
            market.write_after_tick_to_arctic(instrument_, tick_)

        trade_set = set()
        trade = market.get_trade(instrument_, '2017-01-01 00:00:00', '2017-10-15 20:30:00')
        for index in trade.index:
            trade_set.add(index.strftime('%Y-%m-%d'))
        for d in trade_set:
            trade_ = market.get_trade(instrument_, d+' 09:00:00', d+' 16:00:00')
            market.write_after_trade_to_arctic(instrument_, trade_)

