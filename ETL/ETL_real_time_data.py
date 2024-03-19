import time
from collections import OrderedDict

from cassandra.cluster import Cluster

import pandas as pd


cluster = Cluster(['localhost'], port=9042)

session = cluster.connect()

keyspace = "vietnam_stock"

session.set_keyspace(keyspace)


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def aggregate_ohlcv(session: Cluster.connect, df: pd.DataFrame, ticker_list: list):
    for ticker in ticker_list:
        ticker_df = df.loc[df['ticker'] == ticker]
        
        ticker_df['trading_time'] = pd.to_datetime(ticker_df['trading_time'])

        ticker_df.set_index('trading_time', inplace=True)

        ohlc_df = ticker_df.resample('1min', label='left', closed='right').agg(
            OrderedDict([
                ('open', 'first'),
                ('high', 'max'),
                ('low', 'min'),
                ('close', 'last'),
                ('volume', 'sum')
            ])
        )

        ohlc_df = ohlc_df.assign(ticker=ticker)

        ohlc_df = ohlc_df.reset_index()
        
        ohlc_df['trading_time'] = pd.to_datetime(ohlc_df['trading_time'])
        
        ohlc_df = ohlc_df.fillna(0)
        
        ohlc_df = ohlc_df.astype(
            {
                "open": "int",
                "high": "int",
                "low": "int",
                "close": "int"
            }
        )
        
        ohlc_df = ohlc_df.loc[:, ['trading_time', 'ticker', 'open', 'high', 'low', 'close', 'volume']]

        insert_statement = session.prepare("""
                                           INSERT INTO real_time_stock_trading_data_one_min(trading_time, ticker, open, high, low, close, volume) 
                                           VALUES (?,?,?,?,?,?,?);
                                           """)
    
        for data in ohlc_df.itertuples(index=False):
            value = tuple(data)
            
            session.execute(insert_statement, value)
        
        print(f"Insert {ticker} successfully!")


def main():    
    ticker_list = ['BID', 'CTG', 'MBB', 'TCB', 'VCB']
    
    session.row_factory = pandas_factory
    
    session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

    rows = session.execute("""select trading_time, ticker, open, high, low, close, volume from real_time_stock_trading_data;""")
    
    df = rows._current_rows
    
    print("Extract real time successfully!")
    
    session.execute("TRUNCATE TABLE real_time_stock_trading_data_one_min;")    
    
    time.sleep(2)
    
    aggregate_ohlcv(session, df, ticker_list)

    time.sleep(2)

    print("Task completed!")


if __name__ == '__main__':
    main()
