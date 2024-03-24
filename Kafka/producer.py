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
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

client = MarketDataClient(config)


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

    json_data = client.index_components(config, model.index_components('vn30', 1, 50))

    df = pd.DataFrame(json_data['data'][0]['IndexComponent'])

    ticker_list = df['StockSymbol'].to_list()
    
    ticker_string = 'B:' + ticker_list[0]
    
    for i in range(1, len(ticker_list), 1):
        ticker_string += '-' + ticker_list[i]
        
    # ticker = 'B:BID-CTG-MBB-TCB-VCB'
    
    try:
        stream.start(get_market_data, get_error, ticker_string)

        message = None
    
        while message != "exit()":
            message = input()
    
            if message is not None and message != "" and message != "exit()":
                stream.swith_channel(message)

    except Exception as e:
        print(f"Error here: {e}")
        
        
main()
