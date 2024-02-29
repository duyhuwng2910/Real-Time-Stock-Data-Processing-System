import datetime
import random
import time

import pandas as pd
import vnstock
import mysql.connector
from sqlalchemy import create_engine, types

connection = mysql.connector.connect(user='root',
                                     password='root',
                                     host='localhost',
                                     database='demo')

cursor = connection.cursor()

# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:root@localhost/demo")


def extract_companies_list_default_data():
    print("Starting extracting default companies list data...")

    df = vnstock.listing_companies(live=False)

    df.rename(columns={'comGroupCode': 'com_group_code',
                       'organName': 'organ_name',
                       'organShortName': 'organ_short_name',
                       'organTypeCode': 'organ_type_code',
                       'comTypeCode': 'com_type_code',
                       'icbName': 'icb_name',
                       'icbNamePath': 'icb_name_path',
                       'group': 'group_name',
                       'subgroup': 'sub_group',
                       'icbCode': 'icb_code'},
              inplace=True)

    # print(df.dtypes)

    '''
        First way to insert
    '''
    # Prepare SQL statement with placeholders for values
    cursor.execute("DELETE FROM companies_list_default;")
    
    statement = "INSERT INTO companies_list_default (ticker, com_group_code, organ_name, organ_short_name, " \
                "organ_type_code, com_type_code, icb_name, icb_name_path, sector, industry, group_name, sub_group, " \
                "icb_code, VN30, VNMID, VN100, VNSML, VNALL, HNX30, VNX50, VNXALL, VNDIAMOND, VNFINLEAD, " \
                "VNFINSELECT, VNSI, VNCOND, VNCONS, VNENE, VNFIN, VNHEAL, VNIND, VNIT, VNMAT, VNREAL, VNUTI) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    # Iterate through DataFrame rows and insert values
    try:
        for row in df.itertuples(index=False):
            values = tuple(row)  # Convert DataFrame row to tuple
            
            cursor.execute(statement, values)
            
        print("Insert default companies list data completely!")
    
        return df
    
    except mysql.connector.Error as err:
        print("Error while inserting row:", err)

    '''
        Second way to insert
    '''
    # # Cấu hình kiểu dữ liệu cho các cột
    # sql_type = {
    #     'ticker': types.VARCHAR(255),
    #     'com_group_code': types.VARCHAR(255),
    #     'organ_name': types.VARCHAR(255),
    #     'organ_short_name': types.VARCHAR(255),
    #     'organ_type_code': types.VARCHAR(255),
    #     'com_type_code': types.VARCHAR(255),
    #     'icb_name': types.VARCHAR(255),
    #     'icb_name_path': types.VARCHAR(255),
    #     'sector': types.VARCHAR(255),
    #     'industry': types.VARCHAR(255),
    #     'group_name': types.VARCHAR(255),
    #     'sub_group': types.VARCHAR(255),
    #     'icb_code': types.BIGINT,
    #     'VN30': types.Boolean(),
    #     'VNMID': types.Boolean(),
    #     'VN100': types.Boolean(),
    #     'VNSML': types.Boolean(),
    #     'VNALL': types.Boolean(),
    #     'HNX30': types.Boolean(),
    #     'VNX50': types.Boolean(),
    #     'VNXALL': types.Boolean(),
    #     'VNDIAMOND': types.Boolean(),
    #     'VNFINLEAD': types.Boolean(),
    #     'VNFINSELECT': types.Boolean(),
    #     'VNSI': types.Boolean(),
    #     'VNCOND': types.Boolean(),
    #     'VNCONS': types.Boolean(),
    #     'VNENE': types.Boolean(),
    #     'VNFIN': types.Boolean(),
    #     'VNHEAL': types.Boolean(),
    #     'VNIND': types.Boolean(),
    #     'VNIT': types.Boolean(),
    #     'VNMAT': types.Boolean(),
    #     'VNREAL': types.Boolean(),
    #     'VNUTI': types.Boolean()
    # }

    # try:
    #     df.to_sql('companies_list_default', con=engine, if_exists='replace', index=False,
    #               index_label='ticker',
    #               dtype=sql_type)

    #     print("Insert default companies list data completely!")

    #     return df

    # except Exception as e:
    #     print(f"Error here:{e}")


def extract_companies_list_live_data():
    print("Starting extracting live companies list data...")

    df = vnstock.listing_companies(live=True, source='SSI')

    df.rename(columns={'organCode': 'organ_code',
                       'comGroupCode': 'com_group_code',
                       'icbCode': 'icb_code',
                       'organTypeCode': 'organ_type_code',
                       'comTypeCode': 'com_type_code',
                       'organName': 'organ_name',
                       'organShortName': 'organ_short_name'},
              inplace=True)

    try:
        df.to_sql('companies_list_live', con=engine, if_exists='replace', index=False,
                  index_label='ticker',
                  dtype={
                      'organ_code': types.VARCHAR(50),
                      'ticker': types.VARCHAR(15),
                      'com_group_code': types.VARCHAR(10),
                      'icb_code': types.VARCHAR(50),
                      'organ_type_code': types.VARCHAR(5),
                      'com_type_code': types.VARCHAR(10),
                      'organ_name': types.VARCHAR(255),
                      'organ_short_name': types.VARCHAR(255)
                  })

        print("Insert live companies list completely!")

        return df

    except Exception as e:
        print(f"Error here:{e}")


def extract_companies_overview_data(df: pd.DataFrame):
    print("Starting extracting companies overview data...")

    ticker_df = df['ticker']

    structure = {
        "ticker": pd.Series([], dtype="object"),
        "exchange": pd.Series([], dtype="object"),
        "industry": pd.Series([], dtype="object"),
        "companyType": pd.Series([], dtype="object"),
        "noShareholders": pd.Series([], dtype="int64"),
        "foreignPercent": pd.Series([], dtype="float64"),
        "outstandingShare": pd.Series([], dtype="float64"),
        "issueShare": pd.Series([], dtype="float64"),
        "establishedYear": pd.Series([], dtype="object"),
        "noEmployees": pd.Series([], dtype="int64"),
        "stockRating": pd.Series([], dtype="float64"),
        "deltaInWeek": pd.Series([], dtype="float64"),
        "deltaInMonth": pd.Series([], dtype="float64"),
        "deltaInYear": pd.Series([], dtype="float64"),
        "shortName": pd.Series([], dtype="object"),
        "industryEn": pd.Series([], dtype="object"),
        "industryID": pd.Series([], dtype="int64"),
        "industryIDv2": pd.Series([], dtype="object"),
        "website": pd.Series([], dtype="object"),
    }

    # Create an empty DataFrame with the specified schema
    co_df = pd.DataFrame(structure)

    error_df = pd.DataFrame({'ticker': []})

    for ticker in ticker_df:
        try:
            company_overview = vnstock.company_overview(ticker)

            co_df = pd.concat([co_df, company_overview])

            print(ticker)
        except Exception as e:
            error_df.loc[len(error_df), 'ticker'] = ticker

            print(f"Error while ingesting {ticker} data:{e}")

            continue

    co_df.rename(columns={
        'exchange': 'exchange_name',
        'companyType': 'company_type',
        'noShareholders': 'number_of_shareholders',
        'foreignPercent': 'foreign_percent',
        'outstandingShare': 'outstanding_share',
        'issueShare': 'issue_share',
        'establishedYear': 'established_year',
        'noEmployees': 'number_of_employees',
        'stockRating': 'stock_rating',
        'deltaInWeek': 'delta_in_week',
        'deltaInMonth': 'delta_in_month',
        'deltaInYear': 'delta_in_year',
        'shortName': 'short_name',
        'industryEn': 'industry_en',
        'industryID': 'industry_id',
        'industryIDv2': 'industry_id_v2'
    },
        inplace=True)

    try:
        co_df.to_sql('companies_overview', con=engine, if_exists='replace', index=False,
                     index_label='ticker',
                     dtype={
                        'ticker': types.VARCHAR(255),
                        'exchange_name': types.VARCHAR(255),
                        'industry': types.VARCHAR(255),
                        'company_type': types.VARCHAR(255),
                        'no_share_holders': types.BIGINT,
                        'foreign_percent': types.FLOAT,
                        'out_standing_share': types.FLOAT,
                        'issue_share': types.FLOAT,
                        'established_year': types.VARCHAR(255),
                        'no_employees': types.BIGINT,
                        'stock_rating': types.FLOAT,
                        'delta_in_week': types.FLOAT,
                        'delta_in_month': types.FLOAT,
                        'delta_in_year': types.FLOAT,
                        'short_name': types.VARCHAR(255),
                        'industry_en': types.VARCHAR(255),
                        'industry_id': types.INT,
                        'industry_id_v2': types.VARCHAR(255),
                        'website': types.VARCHAR(255)
                     })

        print("Insert companies overview data completely!")

    except Exception as e:
        print(f"Error here:{e}")

    try:
        error_df.to_csv('error_companies_overview_list.csv')

        print("export sucessfully!")
    except Exception as e:
        print(f"Error here:{e}")


def exact_historical_data(df: pd.DataFrame):
    print("Starting extracting stock historical data...")

    ticker_df = df['ticker']

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

    sh_df = pd.DataFrame(columns=columns)

    # Set data types for the columns
    for column, dtype in dtypes.items():
        sh_df[column] = sh_df[column].astype(dtype)

    # For usage purpose
    year = 2024
    # for year in range(2021, 2024, 1):
    for ticker in ticker_df:
        start_date = str(year) + '-01-01'
        end_date = str(year) + '-01-31'

        try:
            stock_historical_data = vnstock.stock_historical_data(symbol=ticker,
                                                                  start_date=start_date,
                                                                  end_date=end_date,
                                                                  resolution='1D',
                                                                  type='stock',
                                                                  beautify=True,
                                                                  decor=False,
                                                                  source='TCBS')
            print(ticker)

        except KeyError:
            continue
        except pd.errors.IntCastingNaNError:
            continue

        sh_df = pd.concat([sh_df, stock_historical_data])

        time.sleep(random.uniform(0.2, 0.4))

    try:
        sh_df.to_sql('stock_historical_data_one_day', con=engine, if_exists='append', index=False)

        print(f"Insert stock historical data of year {year} completely!")

    except Exception as e:
        print(f"Error here:{e}")


def extract_daily_price_stock_data(df: pd.DataFrame):
    print("Starting extracting stock historical data...")

    ticker_df = df['ticker']

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

    sh_df = pd.DataFrame(columns=columns)

    # Set data types for the columns
    for column, dtype in dtypes.items():
        sh_df[column] = sh_df[column].astype(dtype)

    end_date = datetime.date.today().strftime('%Y-%m-%d')
    start_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d')
                  - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    for ticker in ticker_df:
        try:
            stock_historical_data = vnstock.stock_historical_data(symbol=ticker,
                                                                  start_date=start_date,
                                                                  end_date=end_date,
                                                                  resolution='1D',
                                                                  type='stock',
                                                                  beautify=True,
                                                                  decor=False,
                                                                  source='TCBS')
            print(ticker)

        except KeyError:
            continue
        except pd.errors.IntCastingNaNError:
            continue

        sh_df = pd.concat([sh_df, stock_historical_data])

        time.sleep(random.uniform(0.2, 0.4))

    try:
        sh_df.to_sql('stock_historical_data_one_day', con=engine, if_exists='append', index=False)

        print(f"Insert daily stock price data completely!")

    except Exception as e:
        print(f"Error here:{e}")


def extract_general_rating_data(df: pd.DataFrame):
    print("Starting extracting general rating data...")

    ticker_df = df['ticker']

    columns = ['stockRating', 'valuation', 'financialHealth', 'businessModel',
               'businessOperation', 'rsRating', 'taScore', 'ticker', 'highestPrice',
               'lowestPrice', 'priceChange3m', 'priceChange1y', 'beta', 'alpha']

    # Cấu trúc bảng SQL
    dtypes = {
        "stockRating": "float",
        "valuation": "float",
        "financialHealth": "float",
        "businessModel": "float",
        "businessOperation": "float",
        "rsRating": "float",
        "taScore": "float",
        "ticker": "object",
        "highestPrice": "float",
        "lowestPrice": "float",
        "priceChange3m": "float",
        "priceChange1y": "float",
        "beta": "float",
        "alpha": "float",
    }

    gr_df = pd.DataFrame(columns=columns)

    # Set data types for the columns
    for column, dtype in dtypes.items():
        gr_df[column] = gr_df[column].astype(dtype)

    for ticker in ticker_df:
        try:
            general_rating = vnstock.general_rating(ticker)

            print(ticker)

            gr_df = pd.concat([gr_df, general_rating])

        except KeyError:
            continue

    gr_df = gr_df[['ticker', 'stockRating', 'valuation', 'financialHealth', 'businessModel',
                   'businessOperation', 'rsRating', 'taScore', 'highestPrice',
                   'lowestPrice', 'priceChange3m', 'priceChange1y', 'beta', 'alpha']]

    gr_df.rename(columns={
        'stockRating': 'stock_rating',
        'financialHealth': 'financial_health',
        'businessModel': 'business_model',
        'businessOperation': 'business_operation',
        'rsRating': 'rs_rating',
        'taScore': 'ta_score',
        'highestPrice': 'highest_price',
        'lowestPrice': 'lowest_price',
        'priceChange3m': 'price_change_3m',
        'priceChange1y': 'price_change_1y'
    },
        inplace=True)

    try:
        gr_df.to_sql('general_rating', con=engine, if_exists='replace', index=False,
                     index_label='ticker')

        print("Insert general rating data completely!")

    except Exception as e:
        print(f"Error here:{e}")


def main():
    # cld_df = extract_companies_list_default_data()

    cll_df = extract_companies_list_live_data()

    # extract_companies_overview_data(cll_df)

    # exact_historical_data(cld_df)

    # extract_general_rating_data(cll_df)

    connection.commit()

    connection.close()


if __name__ == "__main__":
    main()
