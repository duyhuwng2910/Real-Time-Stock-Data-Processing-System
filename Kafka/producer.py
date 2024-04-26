import time
import json
import sys
import pandas as pd

from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from ssi_fc_data import model

import vnstock_data

from kafka import KafkaProducer

from sqlalchemy import create_engine

# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

bootstrap_servers = ['localhost:29093', 'localhost:29094']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

client = MarketDataClient(config)

vnstock_client = client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/vietnam_stock")


# fucntion to get data message and send to Kafka topic
def get_market_data(message):
    trading_info = message['Content']

    data = json.loads(trading_info)

    producer.send('stock', data)


# function to get error
def get_error(error):
    print(error)


def main():
    print("Starting extracting real time stock trading data...")
    
    time.sleep(2)
    
    stream = MarketDataStream(config, MarketDataClient(config))
    
    # ticker = input("Please type the ticker you want to extract real time data:")

    # Return the data of ticker list in VN30
    # df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)
        
    # Return the data of ticker list in VN100
    # df = vnstock_data.ssi.get_index_component(client, config, index='VN100', page=1, pageSize=100)

    # Return the data of of all ticker in HOSE exchange
    stock_df = pd.read_excel(
        'W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx',
        sheet_name='Stock')
    
    hose_df = stock_df.loc[stock_df['exchange'] == 'HOSE']
    
    df = hose_df['ticker']
    
    df.to_sql('stock_list', engine, if_exists='replace', index=False)

    ticker_list = df.to_list()
    
    # ticker_list = df['StockSymbol'].to_list()
    
    ticker_string = 'B:' + ticker_list[0]
    
    for i in range(1, len(ticker_list), 1):
        ticker_string += '-' + ticker_list[i]
    
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
