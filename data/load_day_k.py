import sys
sys.path.append('/home/market/code/market_data/')
from common.market_config import MarketConfig
from os.path import os
from Queue import Queue
from threading import Thread
import pandas as pd
from common.data_source_factory import DataSourceFactory
import pytz
from datetime import datetime

DAY_SURFIX= '_DAY'
tz = pytz.timezone('Asia/Shanghai')
queue = Queue()
root_path = MarketConfig.get('FILE_PATH', 'DAY_K')
files = []
fields = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
market = DataSourceFactory.get_data_source()
market.create_collection('SH_DAY')
market.create_collection('SZ_DAY')

def do_job():
    while True:
        f = queue.get()
        try:
            ohlc = pd.read_csv(f, sep=',', header=None, names=fields)
            file_arr = str(f).strip().split("/")
            symbol = file_arr[len(file_arr)-1].upper().replace("SH","SH.").replace("SZ","SZ.").replace(".CSV","")

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

            table = symbol.split('.')[0] + DAY_SURFIX
            market.write_data_to_arctic(symbol, ohlc, table)
            queue.task_done()
        except Exception as e:
            print str(f)+' fail run again ' + str(e.message)
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

