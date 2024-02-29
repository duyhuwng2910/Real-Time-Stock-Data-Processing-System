import threading

import pandas as pd

import vnstock
import vnstock_data.ssi as ssi
import SSI.config as config

import time
import random
import mysql.connector
from sqlalchemy import create_engine

connection = mysql.connector.connect(user='root',
                                     password='root',
                                     host='localhost',
                                     database='demo')

cursor = connection.cursor()

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/demo")

client = ssi.fc_md_client.MarketDataClient(config)


def exact_historical_data(df: pd.DataFrame):
    print("Starting extracting stock historical data...")

    ticker_df = df['ticker']

    columns = ['Symbol', 'Market', 'TradingDate', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value']

    dtypes = {
        'Symbol': 'object',
        'Market': 'object',
        'TradingDate': 'datetime64[ns]',
        'Open': 'int64',
        'High': 'int64',
        'Low': 'int64',
        'Close': 'int64',
        'Volume': 'int64',
        'Value': 'float64'
    }

    sh_df = pd.DataFrame(columns=columns)

    # Set data types for the columns
    for column, dtype in dtypes.items():
        sh_df[column] = sh_df[column].astype(dtype)

    # for year in range(2021, 2024, 1):
    for ticker in ticker_df:
        try:
            stock_historical_data = ssi.get_daily_ohlc(client,
                                                       config,
                                                       symbol=ticker,
                                                       fromDate='01/01/2024',
                                                       toDate='29/02/2024',
                                                       ascending=True,
                                                       page=1,
                                                       pageSize=5000)
            print(ticker)

        except KeyError:
            print(f"Key error with {ticker}")
            continue
        except pd.errors.IntCastingNaNError:
            print(f"NA error with {ticker}")
            continue

        stock_historical_data['TradingDate'] = pd.to_datetime(stock_historical_data['TradingDate'], format='%d/%m/%Y')

        sh_df = pd.concat([sh_df, stock_historical_data])
    try:
        sh_df.to_sql('test', con=engine, if_exists='append', index=False)

        print(f"Insert stock historical data completely!")

    except Exception as e:
        print(f"Error here:{e}")


def main():
    df = pd.read_excel('/home/nguyenduyhung/graduation_thesis/project/Excel files/vn_stock.xlsx', sheet_name='Stock')

    hose_df = df.loc[df['exchange'] == 'HOSE']

    hnx_df = df.loc[df['exchange'] == 'HNX']

    upcom_df = df.loc[df['exchange'] == 'UPCOM']

    threads_list = []

    exchange_df_list = [hose_df, hnx_df, upcom_df]

    for exchange_df in exchange_df_list:
        thread = threading.Thread(target=exact_historical_data, args=(exchange_df,))

        threads_list.append(thread)

        thread.start()

    for thread in threads_list:
        thread.join()

    print("Done")

    # df = ssi.get_daily_ohlc(client, config, symbol='SSI', fromDate='01/01/2024', toDate='29/02/2024', ascending=True, page=1, pageSize=6000)
    #
    # print(df.dtypes)


main()
