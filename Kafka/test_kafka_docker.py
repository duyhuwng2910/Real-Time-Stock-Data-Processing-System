import time

from SSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

import json

from kafka import KafkaProducer

bootstrap_servers = ['localhost:29093', 'localhost:29094']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
trade_list = []


# get market data message
def get_market_data(message):
    trade_list.append(message)

    print(message)

    kafka_message = producer.send('demo', message)

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


# main function
def main():
    mm = MarketDataStream(config, MarketDataClient(config))

    mm.start(get_market_data, get_error, 'B:VIC')

    while True:
        try:
            print("here")

            time.sleep(1)

        except KeyboardInterrupt:
            break

    producer.flush()


main()
