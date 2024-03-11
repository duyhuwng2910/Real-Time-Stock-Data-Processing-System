import threading
import json
import sys

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

from kafka import KafkaProducer

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

ticker_list = ['VIC', 'VHM', 'VRE', 'MSN', 'SSI', 'TCB', 'VCB', 'BID', 'HPG', 'GAS', 'FPT']


# get market data message
def get_market_data(message):
    trading_info = message['Content']

    data = json.loads(trading_info)

    print(data)

    kafka_message = producer.send('demo', data)

    # Chờ phản hồi
    record_metadata = kafka_message.get(5)

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")


# get error
def get_error(error):
    print(error)


def extract_real_time_stock_trading_data(stream: MarketDataStream, ticker: str):
    ticker = 'B:' + ticker

    stream.start(get_market_data, get_error, ticker)

    while True:
        pass


def main():
    threads_list = []

    for ticker in ticker_list:
        stream = MarketDataStream(config, MarketDataClient(config))

        thread = threading.Thread(target=extract_real_time_stock_trading_data, args=(stream, ticker))

        threads_list.append(thread)

        thread.start()

    for thread in threads_list:
        thread.join()

    print("Finished")


main()
