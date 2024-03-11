import threading
import json
import sys
import pandas as pd
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from ssi_fc_data import model

from kafka import KafkaProducer

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

client = MarketDataClient(config)


# get market data message
def get_market_data(message):
    trading_info = message['Content']

    data = json.loads(trading_info)

    print(data)

    kafka_message = producer.send('hose', data)

    # Chờ phản hồi
    record_metadata = kafka_message.get(5)

    # Kiểm tra phản hồi
    if record_metadata.topic == 'hose':
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
    # Uncomment the below line if you use Ubuntu
    # df = pd.read_excel('/home/nguyenduyhung/graduation_thesis/project/Excel files/vn_stock.xlsx', sheet_name='Stock')

    # Uncomment the below line if you use Windows
    # df = pd.read_excel(
    #     'W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx',
    #     sheet_name='Stock')

    # df['first_trading_date'] = df['first_trading_date'].dt.strftime('%Y-%m-%d')
    #
    # hose_df = df.loc[df['exchange'] == 'HOSE']
    #
    # hose_ticker_df = hose_df['ticker']

    json_data = client.index_components(config, model.index_components('vn30', 1, 50))

    df = pd.DataFrame(json_data['data'][0]['IndexComponent'])

    ticker_list = df['StockSymbol'].to_list()

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
