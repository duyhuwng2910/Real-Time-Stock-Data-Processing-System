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
from ssi_fc_data import fc_md_client, model
import sys

# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

mysql_host = "localhost"
mysql_user = "root"
mysql_password = "root"
mysql_db = "vietnam_stock"

# If using python-mysql connector library
connection = mysql.connector.connect(user='root',
                                     password='root',
                                     host='localhost',
                                     database='vietnam_stock')

cursor = connection.cursor()

connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}"


# If using SQLAlchemy, create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/vietnam_stock")

# Uncomment the below line if you use Ubuntu
# df = pd.read_excel('/home/nguyenduyhung/graduation_thesis/project/Excel files/vn_stock.xlsx', sheet_name='Stock')

# Uncomment the below line if you use Windows
stock_df = pd.read_excel(
    'W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx',
    sheet_name='Stock')

ticker_df = stock_df['ticker']

cursor.execute('DELETE FROM real_time_stock_trading_data_one_min;')

connection.commit()

# for ticker in ticker_df:
#     df = pd.read_sql(f"SELECT * FROM intraday_stock_data WHERE ticker = '{ticker}' ORDER BY time ASC", con=connection_string)

#     df['time'] = pd.to_datetime(df['time'])

#     df.set_index('time', inplace=True)

#     ohlc_df = df.resample('3min').agg(
#         OrderedDict([
#             ('open', 'first'),
#             ('high', 'max'),
#             ('low', 'min'),
#             ('close', 'last'),
#             ('volume', 'sum'),
#         ])
#     )
    
#     ohlc_df['ticker'] = df['ticker']
    
#     ohlc_df.to_sql('intraday_stock_data_three_mins', con=engine, if_exists='append', index=False)

# print("done")

df = pd.read_sql(f"SELECT trading_time, ticker, open, high, low, close, volume FROM real_time_stock_trading_data WHERE ticker = 'VPB' ORDER BY trading_time ASC", con=connection_string)

df['trading_time'] = pd.to_datetime(df['trading_time'])

df.set_index('trading_time', inplace=True)

ohlc_df = df.resample('1min', label='left', closed='right').agg(
    OrderedDict([
        ('open', 'first'),
        ('high', 'max'),
        ('low', 'min'),
        ('close', 'last'),
        ('volume', 'sum')
    ])
)

ohlc_df = ohlc_df.assign(ticker='VPB')

ohlc_df = ohlc_df.reset_index()

print(ohlc_df)

ohlc_df.to_sql('real_time_stock_trading_data_one_min', con=engine, if_exists='append', index=False)