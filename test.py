import time

from SSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

import json

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf8'))

trade_list = []


# get market data message
def get_market_data(message):
    trade_list.append(message)

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
