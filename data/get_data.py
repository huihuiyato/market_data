#-*- coding=utf-8 -*-
from common.data_source_factory import DataSourceFactory

if __name__ == '__main__':
    market = DataSourceFactory.get_data_source()
    # ts = market.list_libraries()

    # syms = market.list_symbols('SZ_MINUTES')
    # print syms

    # day_k = market.day_k_bars('880950.SH', '2014-08-01', '2017-08-30')
    # print day_k

    # close = market.close_price('880950.SH', '2017-08-28')
    # print close

    # date = market.last_trade_date('000552.SZ', 'minute')
    # print date

    minute_k = market.minute_k_bars('399984.SZ')
    print minute_k

    # tick = market.get_tick('000552.SZ', '2017-08-29 01:00:00', '2017-08-29 20:30:00')
    # print tick

    # trade = market.get_trade('000552.SZ', '2017-01-01 01:00:00', '2017-03-27 20:30:00')
    # print trade

    # dividend = market.get_dividend('000895.SZ')
    # print dividend

    # equity = market.get_equity('600006.SH', date = '2009-03-01 01:00:00')
    # print equity

    # finance = market.get_finance('600006.SH', date = '2017-03-01 01:00:00')
    # print finance.loc[:, [u'时间', u'投资收益']]

