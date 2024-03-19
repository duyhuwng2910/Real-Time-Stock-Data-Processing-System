import threading
import json
import random
import time

import pandas as pd

from cassandra.cluster import Cluster
from kafka import KafkaProducer

'''
    Cassandra configuration
'''
cluster = Cluster(['localhost'], port=9042)

session = cluster.connect()

keyspace = "vietnam_stock"

session.set_keyspace(keyspace)

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


'''
    Kafka configuration
'''
bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

ticker_list = [ 'BID', 'CTG', 'MBB' 'TCB', 'VCB']


def send_message(df: pd.DataFrame):
    tl = df.to_dict('records')
    
    for row in tl:
        row['trading_time'] = str(row['trading_time'])

        producer.send('hose', row)
        
        time.sleep(random.random())    

def main():
    session.row_factory = pandas_factory
    
    session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

    rows = session.execute("""
                           SELECT ticker, trading_time, open, high, low, close, volume 
                           FROM real_time_stock_trading_data;""")
    
    df = rows._current_rows
    
    print("Extract real time successfully!")
    
    threads_list = []

    for ticker in ticker_list:
        ticker_df = df.loc[df['ticker'] == ticker]
        
        thread = threading.Thread(target=send_message, args=(ticker_df,))
        
        threads_list.append(thread)

    for thread in threads_list:
        thread.start()

    for thread in threads_list:
        thread.join()

    print("Finished")


main()
