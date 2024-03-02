import pandas as pd
import vnstock_data
from ssi_fc_data import fc_md_client , model
import sys
import datetime

sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

import config

client = fc_md_client.MarketDataClient(config)


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

ld = get_latest_trading_date()

print(ld)

# df = vnstock_data.get_intraday_ohlc(client, config, symbol='ABS', fromDate=ld, toDate=ld, page=1, pageSize=8000, ascending=True, resolution=1)

# df['trading_time'] = df['TradingDate'].combine(df['Time'], lambda x, y: pd.to_datetime(x + " " + y))

# df['TradingDate'] = pd.to_datetime(df['TradingDate'], format="%d/%m/%Y")

# df = df.rename(columns={'Symbol': 'symbol',
#                         'Value': 'value',
#                         'Open': 'open',
#                         'High': 'high',
#                         'Low': 'low',
#                         'Close': 'close',
#                         'Volume': 'volume'}
#                     )

# df = df.astype(
#                 {'value': 'int64',
#                  'open': 'int64',
#                  'high': 'int64',
#                  'low': 'int64',
#                  'close': 'int64',
#                  'volume': 'uint64'}
#                 )

# in_df = df[['symbol', 'trading_time', 'open', 'high', 'low', 'close', 'volume']]

# print(in_df)

# json_data = client.intraday_ohlc(config, model.intraday_ohlc('ABS', ld, ld, 1, 500, True, 1))

# print(json_data)

# df = pd.DataFrame.from_records(json_data['data'])

# print(df.dtypes)