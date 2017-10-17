#-*- coding=utf-8 -*-

import json
import pytz
from arctic import Arctic
from arctic.date import DateRange
from pandas import DataFrame
from datetime import datetime
from common.base_data_source import DataSource
from common.market_time import decode_datetime
from common.market_time import decode_date
import pandas as pd

DB_PREFIX = "arctic"
tz = pytz.timezone('Asia/Shanghai')
DAY_SURFIX = '_DAY'
MINUTE_SURFIX = '_MINUTES'
ARCTIC_QUOTA = 1024 * 1024 * 1024 * 1024

class ArcticDataSource(DataSource):

    def __init__(self, mongo_connect):
        Arctic._MAX_CONNS = 50
        self.__arctic = Arctic(mongo_connect, connectTimeoutMS=30*1000)

    def close(self):
        self.__arctic.reset()

    def create_collection(self, collection_name):
        '''
        create arctic tickstore collection
        :param collection_name:
        :return: None
        '''
        libs = self.__arctic.list_libraries()
        if not collection_name in libs:
            self.__arctic.set_quota(collection_name, ARCTIC_QUOTA)
            self.__arctic.initialize_library(collection_name, lib_type='TickStoreV3')

    def list_libraries(self):
        '''
        list all the table of arctic database
        :return: list
        '''
        libs = self.__arctic.list_libraries()
        return libs

    def list_symbols(self, lib_name):
        '''
        list all the stock code
        :param lib_name: eg: SZ_DAY
        :return: list
        '''
        return self.__arctic[lib_name].list_symbols()

    def close_price(self, symbol, trade_date):
        '''
        'get close price by day
        :param symbol: stock symbol eg: 399003.SZ
        :param trade_date eg: 2017-07-09
        :return: float: close price
        '''
        symbol = self.reverse_symbol(symbol)
        table = symbol.split('.')[0] + DAY_SURFIX
        lib = self.__arctic[table]
        date_range = DateRange(start=decode_date(trade_date), end=decode_date(trade_date))
        return lib.read(symbol, date_range=date_range)['close'].values[0]

    def last_trade_date(self, symbol, k_type):
        '''
        'get last trade date
        :param symbol: stock symbol eg: 399003.SZ
        :param trade_date eg: day or minute
        :return: str: date
        '''
        if k_type == 'minute':
            df = self.minute_k_bars(symbol)
        elif k_type == 'day':
            df = self.day_k_bars(symbol)
        date = df.iloc[-1:].index.values[0]
        ts = pd.to_datetime(str(date))
        d = ts.strftime('%Y-%m-%d')
        return d

    def day_k_bars(self, symbol, start = '1900-01-01', end = datetime.now().strftime("%Y-%m-%d")):
        '''
        'get day k from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = symbol.split('.')[0] + DAY_SURFIX
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_date(start), end = decode_date(end))
        return lib.read(symbol, date_range = date_range)

    def minute_k_bars(self, symbol, start = '1900-01-01 00:00:00', end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
        '''
        'get minute k from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = symbol.split('.')[0] + MINUTE_SURFIX
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_datetime(start), end = decode_datetime(end))
        return lib.read(symbol, date_range = date_range)

    def get_tick(self, symbol, start = None, end = None, type = None):
        '''
        'get tick data from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :param type: if type=None gets the raw data, else gets the calculated data
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = 'TICK' if type is None else 'TICK_AFTER'
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_datetime(start), end = decode_datetime(end))
        return lib.read(symbol, date_range = date_range)

    def get_trade(self, symbol, start = None, end = None, type = None):
        '''
        'get trade data from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :param type: if type=None gets the raw data, else gets the calculated data
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = 'TRADE' if type is None else 'TRADE_AFTER'
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_datetime(start), end = decode_datetime(end))
        return lib.read(symbol, date_range = date_range)

    def get_dividend(self, symbol, start_date = '1900-01-01 00:00:00', end_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
        '''
        'get data from dividend
        :param symbol: 399003.SZ
        :param start_date: start datetime eg: start_date='2013-03-01 01:00:00' or None
        :param end_date: eg: start_date='2015-03-01 01:00:00' or None
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        records = self.__arctic._conn.get_database(DB_PREFIX).get_collection('DIVIDEND').find({'symbol': symbol, 'datetime' :{'$gte': start_date, '$lte': end_date}}, {'_id': 0}).sort('datetime')
        df = DataFrame(list(records))
        return df

    def get_equity(self, symbol, date = None):
        '''
        'get data from equity
        :param symbol: 399003.SZ
        :param date_str: datetime eg: start_date='2013-03-01 01:00:00'
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        records = None
        if date is None:
            records = self.__arctic._conn.get_database(DB_PREFIX).get_collection('EQUITY').find(
                {'symbol': symbol}, {'_id': 0}).sort([("datetime", -1)])
        else:
            records = self.__arctic._conn.get_database(DB_PREFIX).get_collection('EQUITY').find(
            {'symbol': symbol, 'datetime': {'$lte': date}}, {'_id': 0}).sort([("datetime",-1)]).limit(1)
        df = DataFrame(list(records))
        return df

    def get_finance(self, symbol , date = None):
        '''
        'get data from finance
        :param symbol: 399003.SZ
        :param date_str: datetime eg: start_date='2013-03-01 01:00:00'
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        records = None
        if date is None:
            records = self.__arctic._conn.get_database(DB_PREFIX).get_collection('FINANCE').find({'证券代码': symbol},{'_id':0}).sort([("时间",-1)])
        else:
            records = self.__arctic._conn.get_database(DB_PREFIX).get_collection('FINANCE').find({'证券代码':symbol, '时间': {'$lte': date}},{'_id':0}).sort([("时间",-1)]).limit(1)
        df = DataFrame(list(records))
        return df

    def write_finance(self, ohlc, table):
        '''
        'Load the finance data to mongodb
        :param ohlc: pandas dataFrame
        :param table: mongodb table name eg:DIVIDEND
        :return: None
        '''
        if len(ohlc) > 0:
            records = json.loads(ohlc.T.to_json()).values()
            self.__arctic._conn.get_database(DB_PREFIX).get_collection(table).insert(json.loads(json.dumps(records)))

    def write_data_to_arctic(self, symbol, ohlc, table):
        '''
        'Load the pandas dataFrame to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame or list
        :return: None
        '''
        if len(ohlc) > 0:
            lib = self.__arctic[table]
            lib.write(symbol, ohlc)

    def reverse_symbol(self, symbol):
        arr = symbol.split(".")
        return arr[1]+'.'+arr[0]