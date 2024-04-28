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
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

ticker_df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)

for ticker in ticker_df['StockSymbol']:
    print(ticker)

# today = datetime.date.today()

# df = vnstock_data.stock_historical_data(symbol='SSI',
#                                                     start_date='2024-04-26',
#                                                     end_date=str(today),
#                                                     resolution='1',
#                                                     type='stock',
#                                                     beautify=True,
#                                                     decor=False,
#                                                     source='SSI')

# df1 = df[['time', 'ticker', 'close']]

# df1 = df1.rename(columns={"time": "datetime", "close": "price"})

    # df1['trading_date'] = pd.to_datetime(df1['datetime']).dt.date  # Extract date part

    # df1['time'] = pd.to_datetime(df1['datetime']).dt.strftime('%H:%M:%S')  # Extract time part with formatting

    # df2 = df1[['trading_date', 'time', 'ticker', 'price']]

# for row in df1:
#     print(row)
