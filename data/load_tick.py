#encoding: utf-8
import sys
sys.path.append('/home/market/code/market_data/')
from common.market_config import MarketConfig
from os.path import os
from Queue import Queue
from threading import Thread
import pandas as pd
from common.data_source_factory import DataSourceFactory

queue = Queue()
root_path = MarketConfig.get('FILE_PATH', 'TICK')
files = []
fields = ['c_market', 'c_stock_no', 'c_date_time', 'c_price', 'c_current', 'c_money', 'c_vol', 'c_flag','c_buy1_price', 'c_buy2_price','c_buy3_price', 'c_buy4_price', 'c_buy5_price', 'c_sell1_price', 'c_sell2_price', 'c_sell3_price', 'c_sell4_price', 'c_sell5_price', 'c_buy1_quantity', 'c_buy2_quantity', 'c_buy3_quantity', 'c_buy4_quantity','c_buy5_quantity','c_sell1_quantity', 'c_sell2_quantity', 'c_sell3_quantity', 'c_sell4_quantity','c_sell5_quantity']
market = DataSourceFactory.get_data_source()
market.create_collection('TICK')

def do_job():
    while True:
        f = queue.get()
        try:
            df = pd.read_csv(f, sep=',', encoding='gbk', header=None, names=fields)
            df = df.drop(0)
            file_arr = str(f).split("/")
            symbol = file_arr[len(file_arr)-1].split("_")[0].upper().replace("SH","SH.").replace("SZ","SZ.")
            market.write_tick_to_arctic(symbol, df)
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

