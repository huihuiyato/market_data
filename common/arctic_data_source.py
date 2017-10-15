#encoding: utf-8

import json

import pandas as pd
import pytz
from arctic import Arctic
from arctic.date import DateRange
from pandas import DataFrame
from datetime import datetime
from common.base_data_source import DataSource
from common.market_time import decode_datetime
from common.market_time import decode_date

DB_PREFIX = "arctic"
ARCTIC_QUOTA = 1024 * 1024 * 1024 * 1024
tz = pytz.timezone('Asia/Shanghai')

class ArcticDataSource(DataSource):

    def __init__(self, mongodb_connect):
        Arctic._MAX_CONNS = 50
        self.__arctic = Arctic(mongodb_connect, connectTimeoutMS=30*1000)

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

    def write_finance(self, ohlc, table):
        if len(ohlc) > 0:
            records = json.loads(ohlc.T.to_json()).values()
            self.__arctic._conn.get_database(DB_PREFIX).get_collection(table).insert(json.loads(json.dumps(records)))


    def write_minutes_to_arctic(self, symbol, ohlc):
        '''
        'Load the minutes data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            ohlc.amount = ohlc.amount.astype(float)
            ohlc.date = pd.to_datetime(ohlc.date.astype(str) + ' ' + ohlc.time.astype(str))
            ohlc = ohlc.rename_axis({'date':'index'},axis=1)
            ohlc.set_index('index', inplace=True)
            ohlc.pop('time')
            ohlc.index = [tz.localize(datetime(
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").year,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").month,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").day,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").hour,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").minute,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").second)) for x in ohlc.index]
            table = symbol.split('.')[0] + '_MINUTES'
            lib = self.__arctic[table]
            lib.write(symbol, ohlc)

    def get_minutes_bar(self, symbol, start = None, end = None):
        '''
        'get minute k from mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :return: pandas dataFrame
        '''

        symbol = self.reverse_symbol(symbol)

        table = symbol.split('.')[0] + '_MINUTES'
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_datetime(start), end = decode_datetime(end))
        return lib.read(symbol, date_range = date_range)

    def write_day_to_arctic(self, symbol, ohlc):
        '''
        'Load the day K data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            ohlc.amount = ohlc.amount.astype(float)
            ohlc.date = pd.to_datetime(ohlc.date.astype(str))
            ohlc = ohlc.rename_axis({'date': 'index'}, axis=1)
            ohlc.set_index('index', inplace=True)
            ohlc.index = [tz.localize(datetime(
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").year,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").month,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").day,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").hour,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").minute,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").second)) for x in ohlc.index]
            table = symbol.split('.')[0] + '_DAY'
            lib = self.__arctic[table]
            lib.write(symbol, ohlc)

    def get_day_bar(self, symbol, start = None, end = None):
        '''
        'get day k from mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = symbol.split('.')[0]+'_DAY'
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_date(start), end = decode_date(end))
        return lib.read(symbol, date_range = date_range)

    def write_tick_to_arctic(self, symbol, ohlc):
        '''
        'Load the tick data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            ohlc.c_money = ohlc.c_money.astype(float)
            ohlc.c_date_time = pd.to_datetime(ohlc.c_date_time.astype(str))
            ohlc.pop('c_market')
            ohlc.pop('c_stock_no')
            ohlc = ohlc.rename_axis({'c_date_time': 'index'}, axis=1)
            ohlc.set_index('index', inplace=True)
            ohlc.index = [tz.localize(datetime(
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").year,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").month,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").day,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").hour,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").minute,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").second)) for x in ohlc.index]
            lib = self.__arctic['TICK']
            ohlc["index"] = ohlc.index
            ohlc = ohlc.sort_values('index')
            records = ohlc.to_dict('records')
            lib.write(symbol, records)

    def write_after_tick_to_arctic(self, symbol, ohlc):
        '''
        'Load after the calculation tick data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            symbol = self.reverse_symbol(symbol)
            lib = self.__arctic['TICK_AFTER']
            ohlc["index"] = ohlc.index
            ohlc = ohlc.sort_values('index')
            records = ohlc.to_dict('records')
            lib.write(symbol, records)

    def get_tick(self, symbol, start = None, end = None, type = None):
        '''
        'get tick data from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
        :return: pandas dataFrame
        '''
        symbol = self.reverse_symbol(symbol)
        table = 'TICK' if type is None else 'TICK_AFTER'
        lib = self.__arctic[table]
        date_range = DateRange(start = decode_datetime(start), end = decode_datetime(end))
        return lib.read(symbol, date_range = date_range)

    def write_trade_to_arctic(self, symbol, ohlc):
        '''
        'Load the trade data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            ohlc.datetime = pd.to_datetime(ohlc.datetime.astype(str))
            ohlc = ohlc.rename_axis({'datetime': 'index'}, axis=1)
            ohlc.set_index('index', inplace=True)
            ohlc.index = [tz.localize(datetime(
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").year,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").month,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").day,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").hour,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").minute,
                datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").second)) for x in ohlc.index]
            lib = self.__arctic['TRADE']
            ohlc["index"] = ohlc.index
            ohlc = ohlc.sort_values('index')
            records = ohlc.to_dict('records')
            lib.write(symbol, records)

    def write_after_trade_to_arctic(self, symbol, ohlc):
        '''
        'Load after the calculation trade data to mongodb
        :param symbol: stock symbol eg: SZ.399003
        :param ohlc: pandas dataFrame
        :return: None
        '''
        if len(ohlc) > 0:
            symbol = self.reverse_symbol(symbol)
            lib = self.__arctic['TRADE_AFTER']
            ohlc["index"] = ohlc.index
            ohlc = ohlc.sort_values('index')
            records = ohlc.to_dict('records')
            lib.write(symbol, records)

    def get_trade(self, symbol, start = None, end = None, type = None):
        '''
        'get trade data from mongodb
        :param symbol: stock symbol eg: 399003.SZ
        :param start: start time eg: 2017-07-09 09:30:00
        :param end: end time eg: 2017-07-10 09:30:00
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
        :param end_date: SZ.399003 eg: start_date='2015-03-01 01:00:00' or None
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

    def reverse_symbol(self, symbol):
        arr = symbol.split(".")
        return arr[1]+'.'+arr[0]