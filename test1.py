import pandas as pd

import vnstock_data.ssi as ssi

import sys

sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

import config

import datetime

client = ssi.fc_md_client.MarketDataClient(config)

df = pd.read_excel('W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx', sheet_name='Stock')

df['first_trading_date'] = df['first_trading_date'].dt.strftime('%Y-%m-%d')

value = df.iloc[0, df.columns.get_loc('exchange')]

if value == 'HOSE':
    print("ok")
else:
    print("error")