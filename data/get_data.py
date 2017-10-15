#-*- coding=utf-8 -*-
from common.data_source_factory import DataSourceFactory

if __name__ == '__main__':
    market = DataSourceFactory.get_data_source()

    # day_k = market.get_day_bar('000010.SZ', '2014-08-01', '2017-08-30')
    # print day_k

    # minute_k = market.get_minutes_bar('600734.SH', '2016-07-09 09:30:00', '2016-12-01 15:30:00')
    # print minute_k

    tick = market.get_tick('600006.SH', '2017-01-01 00:00:00', '2017-10-1 20:30:00')
    print tick

    # trade = market.get_trade('600006.SH', '2017-03-01 01:00:00', '2017-04-01 09:30:00')
    # print trade

    # dividend = market.get_dividend('000895.SZ')
    # print dividend

    # equity = market.get_equity('600006.SH', date = '2009-03-01 01:00:00')
    # print equity

    # finance = market.get_finance('600006.SH', date = '2017-03-01 01:00:00')
    # print finance.loc[:, [u'时间', u'投资收益']]
