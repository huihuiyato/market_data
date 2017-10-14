#encoding: utf-8
import sys
sys.path.append('/home/market/code/market_data/')
import pandas as pd
from datetime import datetime
from common.data_source_factory import DataSourceFactory
from common.market_config import MarketConfig
from common.market_time import symbol_str
from common.market_time import columns_str

map = {}

dividend_title = ['symbol','datetime','bonus_share','rights','rights_price','dividends']
equity_title = ['symbol','datetime','total_share','total_sold_share','A_share',
              'B_share','other_tradable_share','foreign_share','total_limited_share','country_shareholding',
              'state_owned_shareholding','owned_shareholding','natural_owned_shareholding','other_shareholding','raise_owned_shareholding',
              'outside_owned_shareholding','outside_natural_shareholding','staff_share','preferred_share','change_reason','null_str']
finance_title = []

map.setdefault("FINANCE", finance_title)
map.setdefault("DIVIDEND", dividend_title)
map.setdefault("EQUITY", equity_title)

market = DataSourceFactory.get_data_source()

def finance_time(d):
    s = datetime.strptime(d, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    return s


if __name__ == '__main__':
    help_msg = 'Usage: python %s <FINANCE|DIVIDEND|EQUITY> --config config_path' % sys.argv[0]
    if len(sys.argv) != 4:
        print help_msg
        sys.exit(1)
    c_type = sys.argv[1]
    f = MarketConfig.get('FILE_PATH', c_type)
    names = map.get(c_type)
    df = None
    if 'FINANCE' == c_type:
        df = pd.read_csv(f, sep=r',', encoding='gbk', header=0, low_memory=False)
        df[df.columns[0]] = [symbol_str(str(x)) for x in df[df.columns[0]]]
        df.columns = [columns_str(x) for x in df.columns]
    else:
        df = pd.read_csv(f, sep=r',', encoding='gbk', header=None, names=names, low_memory=False)
        df.symbol = df.symbol.str.upper().str.replace("SZ", 'SZ.').str.replace("SH", 'SH.')
        df.drop(df.index[0], inplace=True)
        if 'EQUITY' == c_type:
            df.pop('null_str')
            df.datetime = [finance_time(x) for x in df.datetime]
    market.write_finance(df, c_type)
