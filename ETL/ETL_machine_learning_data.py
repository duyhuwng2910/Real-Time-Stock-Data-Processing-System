import sys
import datetime
import time

from cassandra.cluster import Cluster

import pandas as pd

import vnstock_data

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

cluster = Cluster(['localhost'], port=9042)

session = cluster.connect()

session.set_keyspace("vietnam_stock")

ticker_df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)

today = datetime.date.today()


def insert_vn_30_list():
    print("Starting to get the list of VN30")

    session.execute("TRUNCATE TABLE vn30_list;")

    insert_statement = session.prepare("INSERT INTO vn30_list (ticker) VALUES (?)")

    try:
        for row in ticker_df.itertuples(index=False):
            values = tuple(row)  # Convert DataFrame row to tuple

            session.execute(insert_statement, values)

        print("Insert list of VN30 ticker successfully!")

    except Exception as e:
        print(f"Error while inserting VN30 ticker list: {e}")


def extract_machine_learning_data():
    print("Starting to extract the stock data of VN30 ticker list since 01-04-2024 for machine learning purpose")

    session.execute("TRUNCATE TABLE stock_data_for_ml;")

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

        ticker_data['trading_time'] = ticker_data['trading_time'].dt.round(freq='min')

        length = len(ticker_data)

        new_cols = {
            'last_one_minute_price': 0,
            'last_one_minute_volume': 0,
            'last_two_minutes_price': 0,
            'last_two_minutes_volume': 0,
            'last_three_minutes_price': 0,
            'last_three_minutes_volume': 0,
            'last_four_minutes_price': 0,
            'last_four_minutes_volume': 0,
            'last_five_minutes_price': 0,
            'last_five_minutes_volume': 0,
            'next_one_minute_price': 0,
            'next_five_minutes_price': 0
        }

        ticker_data = ticker_data.assign(**new_cols)

        for i in ticker_data.index:
            if i == 0:
                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

            elif i == 1:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

            elif i == 2:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

            elif i == 3:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]
                ticker_data.loc[i, "last_three_minutes_price"] = ticker_data.loc[i - 3, "price"]
                ticker_data.loc[i, "last_three_minutes_volume"] = ticker_data.loc[i - 3, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

            elif i == 4:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]
                ticker_data.loc[i, "last_three_minutes_price"] = ticker_data.loc[i - 3, "price"]
                ticker_data.loc[i, "last_three_minutes_volume"] = ticker_data.loc[i - 3, "volume"]
                ticker_data.loc[i, "last_four_minutes_price"] = ticker_data.loc[i - 4, "price"]
                ticker_data.loc[i, "last_four_minutes_volume"] = ticker_data.loc[i - 4, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

            elif length - 5 <= i < length - 1:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]
                ticker_data.loc[i, "last_three_minutes_price"] = ticker_data.loc[i - 3, "price"]
                ticker_data.loc[i, "last_three_minutes_volume"] = ticker_data.loc[i - 3, "volume"]
                ticker_data.loc[i, "last_four_minutes_price"] = ticker_data.loc[i - 4, "price"]
                ticker_data.loc[i, "last_four_minutes_volume"] = ticker_data.loc[i - 4, "volume"]
                ticker_data.loc[i, "last_five_minutes_price"] = ticker_data.loc[i - 5, "price"]
                ticker_data.loc[i, "last_five_minutes_volume"] = ticker_data.loc[i - 5, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]

            elif i == length - 1:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]
                ticker_data.loc[i, "last_three_minutes_price"] = ticker_data.loc[i - 3, "price"]
                ticker_data.loc[i, "last_three_minutes_volume"] = ticker_data.loc[i - 3, "volume"]
                ticker_data.loc[i, "last_four_minutes_price"] = ticker_data.loc[i - 4, "price"]
                ticker_data.loc[i, "last_four_minutes_volume"] = ticker_data.loc[i - 4, "volume"]
                ticker_data.loc[i, "last_five_minutes_price"] = ticker_data.loc[i - 5, "price"]
                ticker_data.loc[i, "last_five_minutes_volume"] = ticker_data.loc[i - 5, "volume"]

            else:
                ticker_data.loc[i, "last_one_minute_price"] = ticker_data.loc[i - 1, "price"]
                ticker_data.loc[i, "last_one_minute_volume"] = ticker_data.loc[i - 1, "volume"]
                ticker_data.loc[i, "last_two_minutes_price"] = ticker_data.loc[i - 2, "price"]
                ticker_data.loc[i, "last_two_minutes_volume"] = ticker_data.loc[i - 2, "volume"]
                ticker_data.loc[i, "last_three_minutes_price"] = ticker_data.loc[i - 3, "price"]
                ticker_data.loc[i, "last_three_minutes_volume"] = ticker_data.loc[i - 3, "volume"]
                ticker_data.loc[i, "last_four_minutes_price"] = ticker_data.loc[i - 4, "price"]
                ticker_data.loc[i, "last_four_minutes_volume"] = ticker_data.loc[i - 4, "volume"]
                ticker_data.loc[i, "last_five_minutes_price"] = ticker_data.loc[i - 5, "price"]
                ticker_data.loc[i, "last_five_minutes_volume"] = ticker_data.loc[i - 5, "volume"]

                ticker_data.loc[i, "next_one_minute_price"] = ticker_data.loc[i + 1, "price"]
                ticker_data.loc[i, "next_five_minutes_price"] = ticker_data.loc[i + 5, "price"]

        insert_ml_statement = session.prepare("""
                                            INSERT INTO stock_data_for_ml 
                                                (trading_time, ticker, price, volume,
                                                last_one_minute_price,
                                                last_one_minute_volume,
                                                last_two_minutes_price,
                                                last_two_minutes_volume,
                                                last_three_minutes_price,
                                                last_three_minutes_volume,
                                                last_four_minutes_price,
                                                last_four_minutes_volume,
                                                last_five_minutes_price,
                                                last_five_minutes_volume,
                                                next_one_minute_price,
                                                next_five_minutes_price)
                                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                           """)

        # Iterate through DataFrame rows and insert values
        try:
            for row in ticker_data.itertuples(index=False):
                values = tuple(row)  # Convert DataFrame row to tuple

                session.execute(insert_ml_statement, values)

            print(f"Insert intraday data of {ticker} completely!")

        except Exception as err:
            print("Error while inserting row:", err)

        stock_trend_analysis_data = ticker_data.tail(5)

        insert_ta_statement = session.prepare("""
                                            INSERT INTO stock_trend_analysis_data 
                                                (trading_time, ticker, price, volume,
                                                last_one_minute_price,
                                                last_one_minute_volume,
                                                last_two_minutes_price,
                                                last_two_minutes_volume,
                                                last_three_minutes_price,
                                                last_three_minutes_volume,
                                                last_four_minutes_price,
                                                last_four_minutes_volume,
                                                last_five_minutes_price,
                                                last_five_minutes_volume,
                                                next_one_minute_price,
                                                next_five_minutes_price)
                                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                           """)

        # Iterate through DataFrame rows and insert values
        try:
            for row in stock_trend_analysis_data.itertuples(index=False):
                values = tuple(row)  # Convert DataFrame row to tuple

                session.execute(insert_ta_statement, values)

            print(f"Insert data for stock analysis completely!")

        except Exception as err:
            print("Error while inserting row:", err)


def main():
    insert_vn_30_list()

    time.sleep(2)

    extract_machine_learning_data()

    time.sleep(2)

    print("Extract successfully!")


if __name__ == '__main__':
    main()
