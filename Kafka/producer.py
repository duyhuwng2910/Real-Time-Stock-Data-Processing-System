import threading
import time

from SSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

import pandas as pd

import json

from kafka import KafkaProducer

stock_df = pd.read_excel('vn_stock.xlsx', sheet_name='Stock')

hose_df = stock_df.loc[stock_df['exchange'] == 'HOSE', :]

hose_tickers = hose_df['ticker'].to_list()

ticker_list = ['VIC', 'VHM', 'VRE', 'MSN', 'SSI', 'TCB', 'VCB', 'BID', 'HPG', 'GAS', 'FPT']

real_time_data = MarketDataStream(config, MarketDataClient(config))

producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


# get market data message
def get_market_data(message):
    print(message)

    kafka_message = producer.send('demo', message)

    # Chờ phản hồi
    record_metadata = kafka_message.get()

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")


# get error
def get_error(error):
    print(error)


def extract_real_time_stock_trading_data(ticker: str):
    real_time_data.start(get_market_data, get_error, ticker)

    message = None

    while message != "exit()":
        message = input()

        if message is not None and message != "" and message != "exit()":
            real_time_data.swith_channel(message)


def main():
    threads = []

    for ticker in ticker_list:
        thread = threading.Thread(target=extract_real_time_stock_trading_data, args=(ticker,))

        threads.append(thread)

        thread.start()

    for thread in threads:
        thread.join()

    print("Finished")
