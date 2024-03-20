import threading
import json
import sys
import pandas as pd

from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from ssi_fc_data import model

from kafka import KafkaProducer

# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


# get market data message
def get_market_data(message):
    trading_info = message['Content']

    data = json.loads(trading_info)

    producer.send('hose', data)


# get error
def get_error(error):
    print(error)


def main():
    stream = MarketDataStream(config, MarketDataClient(config))
    
    # ticker = input("Please type the ticker you want to extract real time data:")
    
    ticker = 'B:BID-CTG-MBB-TCB-VCB'
    
    stream.start(get_market_data, get_error, ticker)

    while True:
        pass


main()
