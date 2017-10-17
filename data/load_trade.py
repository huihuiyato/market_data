import sys
sys.path.append('/home/market/code/market_data/')
from common.market_config import MarketConfig
from os.path import os
from Queue import Queue
from threading import Thread
from common.data_source_factory import DataSourceFactory
import pandas as pd
from common.market_time import time_parse
import pytz
from datetime import datetime
TRADE_TABLE = 'TRADE'

tz = pytz.timezone('Asia/Shanghai')
queue = Queue()
root_path = MarketConfig.get('FILE_PATH', 'TRADE')
files = []
fields = ['datetime', 'price', 'flag', 'amount']
market = DataSourceFactory.get_data_source()
market.create_collection(TRADE_TABLE)

def do_job():
    while True:
        f = queue.get()
        try:
            ohlc = pd.read_csv(f, header=None, names=fields)
            file_arr = str(f).split("/")
            date_str = file_arr[len(file_arr)-2]+' '
            symbol = file_arr[len(file_arr)-1].upper().replace("SH","SH.").replace("SZ","SZ.").replace(".CSV","")
            ohlc.datetime = [time_parse(date_str+str(x)) for x in ohlc.datetime]
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
            ohlc["index"] = ohlc.index
            ohlc = ohlc.sort_values('index')
            records = ohlc.to_dict('records')
            market.write_data_to_arctic(symbol, records, TRADE_TABLE)
            queue.task_done()
        except Exception as e:
            print str(f)+' fail run again ' + str(e)
            queue.task_done()

def file_lists(rootDir):
    for lists in os.listdir(rootDir):
        sub_path = os.path.join(rootDir, lists)
        if sub_path.endswith('.csv'):
            files.append(sub_path)
        if os.path.isdir(sub_path):
            file_lists(sub_path)

if __name__ == '__main__':
    file_lists(root_path)

    for f in files:
        queue.put(str(f))

    for i in range(50):
        t = Thread(target=do_job)
        t.daemon = True
        t.start()

    queue.join()

