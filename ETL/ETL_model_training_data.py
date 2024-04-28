import sys
import datetime
import time

from cassandra.cluster import Cluster

import pandas as pd

import vnstock_data


# Uncomment if you use Windows
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
# sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

cluster = Cluster(['localhost'], port=9042)

session = cluster.connect()

session.set_keyspace("vietnam_stock")

ticker_df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)

today = datetime.date.today()


def insert_vn_30_list():    
    session.execute("TRUNCATE TABLE vn30_list;")
    
    insert_statement = session.prepare("INSERT INTO vn30_list (ticker) VALUES (?)")

    try:
        for row in ticker_df.itertuples(index=False):
            values = tuple(row)  # Convert DataFrame row to tuple
            
            session.execute(insert_statement, values)
        
        print("Insert list of VN30 ticker successfully!")
        
    except Exception as e:
        print(f"Error while inserting VN30 ticker list: {e}")


def insert_intraday_data():
    for ticker in ticker_df['StockSymbol']:
        intraday_data = vnstock_data.stock_historical_data(symbol=ticker,
                                                            start_date='2024-04-01',
                                                            end_date=str(today),
                                                            resolution='1',
                                                            type='stock',
                                                            beautify=True,
                                                            decor=False,
                                                            source='SSI')

        ticker_data = intraday_data[['time', 'ticker', 'close', 'volume']]

        ticker_data = ticker_data.rename(columns={"time": "trading_time", "close": "price"})

        insert_statement = session.prepare("""
                                            INSERT INTO intraday_data (trading_time, ticker, price, volume)
                                            VALUES (?,?,?,?)
                                           """)
        
        # Iterate through DataFrame rows and insert values
        try:
            for row in ticker_data.itertuples(index=False):
                values = tuple(row)  # Convert DataFrame row to tuple
                
                session.execute(insert_statement, values)
                
            print(f"Insert intraday data of {ticker} completely!")

        except Exception as err:
            print("Error while inserting row:", err)
            

def main(): 
    print("Starting to get the list of VN30")
    
    insert_vn_30_list()
    
    time.sleep(2)
    
    print("Starting to get the intraday data since 01-04-2024 for model training purpose")
    
    insert_intraday_data()
    
    time.sleep(2)
    
    print("Insert successfully!")

    
if __name__ == '__main__':
    main()