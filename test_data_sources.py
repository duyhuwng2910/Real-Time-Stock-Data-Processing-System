import threading
import pandas as pd
import datetime
import time
import os
from collections import OrderedDict

import mysql.connector
from sqlalchemy import create_engine, types

import vnstock_data
import vnstock
import sys

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

# ticker_df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)
#
# for ticker in ticker_df['StockSymbol']:
#     print(ticker)

today = datetime.date.today()

df = vnstock_data.stock_historical_data(symbol='SSI',
                                        start_date='2024-04-26',
                                        end_date='2024-04-26',
                                        resolution='1',
                                        type='stock',
                                        beautify=True,
                                        decor=False,
                                        source='SSI')

df1 = df[['time', 'ticker', 'close', 'volume']]

df1 = df1.rename(columns={"time": "datetime", "close": "price"})

df1['datetime'] = df1['datetime'].dt.round(freq='min')

df1['trading_date'] = pd.to_datetime(df1['datetime']).dt.date  # Extract date part

df1['time'] = pd.to_datetime(df1['datetime']).dt.strftime('%H:%M:%S')  # Extract time part with formatting

df2 = df1[['trading_date', 'time', 'ticker', 'price', 'volume']]

length = len(df2)

new_cols = {
    'last_one_minute_price': 0,
    'last_one_minute_volume': 0,
    'last_two_minutes_price': 0,
    'last_two_minutes_volume': 0,
    'last_three_minutes_price': 0,
    'last_three_minutes_volume': 0,
    'last_four_minutes_price': 0,
    'last_four_minutes_volume': 0,
    'last_five_minutes_price': 0,
    'last_five_minutes_volume': 0,
    'next_one_minute_price': 0,
    'next_five_minutes_price': 0
}

df2 = df2.assign(**new_cols)

# print(df2.dtypes)

for i in df2.index:
    if i == 0:
        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

    elif i == 1:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

    elif i == 2:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

    elif i == 3:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]
        df2.loc[i, "last_three_minutes_price"] = df2.loc[i - 3, "price"]
        df2.loc[i, "last_three_minutes_volume"] = df2.loc[i - 3, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

    elif i == 4:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]
        df2.loc[i, "last_three_minutes_price"] = df2.loc[i - 3, "price"]
        df2.loc[i, "last_three_minutes_volume"] = df2.loc[i - 3, "volume"]
        df2.loc[i, "last_four_minutes_price"] = df2.loc[i - 4, "price"]
        df2.loc[i, "last_four_minutes_volume"] = df2.loc[i - 4, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

    elif length - 5 <= i < length - 1:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]
        df2.loc[i, "last_three_minutes_price"] = df2.loc[i - 3, "price"]
        df2.loc[i, "last_three_minutes_volume"] = df2.loc[i - 3, "volume"]
        df2.loc[i, "last_four_minutes_price"] = df2.loc[i - 4, "price"]
        df2.loc[i, "last_four_minutes_volume"] = df2.loc[i - 4, "volume"]
        df2.loc[i, "last_five_minutes_price"] = df2.loc[i - 5, "price"]
        df2.loc[i, "last_five_minutes_volume"] = df2.loc[i - 5, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]

    elif i == length - 1:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]
        df2.loc[i, "last_three_minutes_price"] = df2.loc[i - 3, "price"]
        df2.loc[i, "last_three_minutes_volume"] = df2.loc[i - 3, "volume"]
        df2.loc[i, "last_four_minutes_price"] = df2.loc[i - 4, "price"]
        df2.loc[i, "last_four_minutes_volume"] = df2.loc[i - 4, "volume"]
        df2.loc[i, "last_five_minutes_price"] = df2.loc[i - 5, "price"]
        df2.loc[i, "last_five_minutes_volume"] = df2.loc[i - 5, "volume"]

    else:
        df2.loc[i, "last_one_minute_price"] = df2.loc[i - 1, "price"]
        df2.loc[i, "last_one_minute_volume"] = df2.loc[i - 1, "volume"]
        df2.loc[i, "last_two_minutes_price"] = df2.loc[i - 2, "price"]
        df2.loc[i, "last_two_minutes_volume"] = df2.loc[i - 2, "volume"]
        df2.loc[i, "last_three_minutes_price"] = df2.loc[i - 3, "price"]
        df2.loc[i, "last_three_minutes_volume"] = df2.loc[i - 3, "volume"]
        df2.loc[i, "last_four_minutes_price"] = df2.loc[i - 4, "price"]
        df2.loc[i, "last_four_minutes_volume"] = df2.loc[i - 4, "volume"]
        df2.loc[i, "last_five_minutes_price"] = df2.loc[i - 5, "price"]
        df2.loc[i, "last_five_minutes_volume"] = df2.loc[i - 5, "volume"]

        df2.loc[i, "next_one_minute_price"] = df2.loc[i + 1, "price"]
        df2.loc[i, "next_five_minutes_price"] = df2.loc[i + 5, "price"]

print(df2.dtypes)

print(df2.head(10))
