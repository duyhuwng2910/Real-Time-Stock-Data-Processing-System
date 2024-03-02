import threading
import pandas as pd
import datetime
import time
import os

import mysql.connector
from sqlalchemy import create_engine

import vnstock_data
from ssi_fc_data import fc_md_client , model
import sys

sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

import config


connection = mysql.connector.connect(user='root',
                                     password='root',
                                     host='localhost',
                                     database='demo')

cursor = connection.cursor()

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/demo")

# Create a SSI client
client = fc_md_client.MarketDataClient(config)


'''
    Function to get stock historical data of symbols in a specified exchange 
'''
def extract_daily_ohlcv_data(df: pd.DataFrame):
    ticker_df = df['ticker']

    error_df = pd.DataFrame({'ticker': []})
    
    exchange = df.iloc[0, df.columns.get_loc('exchange')].lower()
    
    df_name = exchange + '_df'
    
    for ticker in ticker_df:
        start_date = df.loc[df['ticker'] == ticker].iloc[0]['first_trading_date']
        
        try:
            stock_historical_data = vnstock_data.stock_historical_data(symbol=ticker,
                                                                  start_date=start_date,
                                                                  end_date=str(datetime.date.today()),
                                                                  resolution='1D',
                                                                  type='stock',
                                                                  beautify=True,
                                                                  decor=False,
                                                                  source='SSI')
            
            stock_historical_data['time'] = pd.to_datetime(stock_historical_data['time'])
            
            try:
                stock_historical_data.to_sql(f'stock_historical_data_one_day_{exchange}', con=engine, if_exists='append', index=False)
                
                print(f"Insert stock historical data of symbol {ticker} completely!")

            except Exception as e:
                print(f"Error here:{e}")
        
        except KeyError:
            print(f"Key error with {ticker}")
            
            error_df.loc[len(error_df), 'ticker'] = ticker
            
            continue
        except pd.errors.IntCastingNaNError:
            print(f"NA error with {ticker}")
            
            error_df.loc[len(error_df), 'ticker'] = ticker
            
            continue
        
    print(f"Insert stock historical data of data frame {df_name} completely!")

    current_dir = os.getcwd()
    
    error_csv_file_path = os.path.join(current_dir, 'Excel files', f'Error files\historical_stock_data_{exchange}_{datetime.date.today()}.csv')
    
    error_df.to_csv(error_csv_file_path)
    
    print("Update error symbols list file successfully!")


'''
    Function to get stock historical data of all of Vietnam Stock Market
'''
def extract_daily_historical_stock_data(exchange_df_list: list):
    print("Starting extracting stock historical data...")
    
    # If you run the first time, please uncomment this below line to run.
    # After running first time, you can comment this line
    cursor.execute('''
                   DROP TABLE IF EXISTS stock_historical_data_one_day_hose;
                   DROP TABLE IF EXISTS stock_historical_data_one_day_hnx;
                   DROP TABLE IF EXISTS stock_historical_data_one_day_upcom;
                   ''')
    
    threads_list = []
    
    for exchange_df in exchange_df_list:
        thread = threading.Thread(target=extract_daily_ohlcv_data, args=(exchange_df,))

        threads_list.append(thread)

        thread.start()

    for thread in threads_list:
        thread.join()

    time.sleep(2)
    
    print("Insert daily historical stock data of Vietnam Stock Market successfully!")


'''
    Function to get the intraday stock data of symbols in a specified exchange
'''
def extract_intraday_ohlcv_data(df: pd.DataFrame, trading_date: str):
    ticker_df = df['ticker']

    error_df = pd.DataFrame({'ticker': []})
    
    exchange = df.iloc[0, df.columns.get_loc('exchange')].lower()
    
    df_name = exchange.upper()
    
    columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'ticker']

    dtypes = {
        'time': 'object',
        'open': 'int64',
        'high': 'int64',
        'low': 'int64',
        'close': 'int64',
        'volume': 'int64',
        'ticker': 'object'
    }
    
    exchange_df = pd.DataFrame(columns=columns)
    
    # Set data types for the columns
    for column, dtype in dtypes.items():
        exchange_df[column] = exchange_df[column].astype(dtype)
    
    for ticker in ticker_df: 
        try:           
            intraday_data = vnstock_data.stock_historical_data(symbol=ticker,
                                                               start_date=str(trading_date),
                                                               end_date=str(trading_date),
                                                               resolution='1',
                                                               type='stock',
                                                               beautify=True,
                                                               decor=False,
                                                               source='SSI')
            
            if intraday_data.shape[0] > 0:
                try:
                    exchange_df = pd.concat([exchange_df, intraday_data])
                
                    print(f"Inserting data of symbol {ticker}")
                except Exception as e:
                    print(f"Error while concat data frame:{e}")
        
        except Exception as e:
            print(f"Error while extracting intraday data of symbol {ticker}:{e}")
            
            error_df.loc[len(error_df), 'ticker'] = ticker
            
            continue
            
    try:
        exchange_df.to_sql(f'intraday_data_one_day_{exchange}', con=engine, if_exists='append', index=False)
        
        print(f"Insert intraday stock data of data frame {df_name} completely!")
    except Exception as e:
        print(f"Error while inserting data: {e}")
        
    current_dir = os.getcwd()
    
    error_csv_file_path = os.path.join(current_dir, 'Excel files', f'Error files\intraday_stock_data_{exchange}.csv')
    
    error_df.to_csv(error_csv_file_path)
    
    print("Update error symbols list file successfully!")


def get_latest_trading_date():
    """
    Trả về ngày hiện tại nếu là từ thứ Hai đến thứ Sáu, hoặc trả về ngày thứ Sáu của tuần đó nếu là thứ Bảy hoặc Chủ Nhật.

    Returns:
    str: Ngày dạng 'yyyy-mm-dd'.
    """
    today = datetime.date.today()
    weekday = today.weekday()
    
    # Nếu là thứ Bảy hoặc Chủ Nhật, trả về ngày thứ Sáu của tuần đó
    if weekday == 5 or weekday == 6:
        return (today - datetime.timedelta(days=(6 - weekday)))

    # Nếu là từ thứ Hai đến thứ Sáu, trả về ngày hiện tại
    return today


'''
    Function to get the intraday stock data of all of Vietnam Stock Market
'''
def extract_intraday_stock_data(exchange_list: list):
    print("Starting extracting intraday stock data...")
    
    latest_trading_date = get_latest_trading_date()

    cursor.execute('''
                   DELETE FROM intraday_stock_data_hose;
                   DELETE FROM intraday_stock_data_hnx;
                   DELETE FROM intraday_stock_data_upcom;
                   ''')
    
    threads_list = []
    
    for exchange in exchange_list:
        thread = threading.Thread(target=extract_intraday_ohlcv_data, args=(exchange,latest_trading_date))
        
        threads_list.append(thread)
        
        thread.start()
        
    for thread in threads_list:
        thread.join()
    
    time.sleep(2)
        
    print("Insert intraday stock data of Vietnam Stock Market successfully!")


def main():
    # Uncomment the below line if you use Ubuntu
    # df = pd.read_excel('/home/nguyenduyhung/graduation_thesis/project/Excel files/vn_stock.xlsx', sheet_name='Stock')
    
    # Uncomment the below line if you use Windows
    df = pd.read_excel('W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx', sheet_name='Stock')
    
    df['first_trading_date'] = df['first_trading_date'].dt.strftime('%Y-%m-%d')
    
    hose_df = df.loc[df['exchange'] == 'HOSE']

    hnx_df = df.loc[df['exchange'] == 'HNX']

    upcom_df = df.loc[df['exchange'] == 'UPCOM']

    exchange_df_list = [hose_df, hnx_df, upcom_df]
    
    # extract_daily_historical_stock_data(exchange_df_list)
    
    extract_intraday_stock_data(exchange_df_list)


main()
