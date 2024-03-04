import pandas as pd
import vnstock_data
from ssi_fc_data import fc_md_client , model
import sys
import datetime
import time

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
    if weekday == 5:
        return (today - datetime.timedelta(days=1)).strftime('%d/%m/%Y')
    elif weekday == 6:
        return (today - datetime.timedelta(days=2)).strftime('%d/%m/%Y')
    else:
        now = datetime.datetime.now()
        
        if now < datetime.datetime.strptime("15:30", "%H:%M"):
            if weekday == 0:
                return today - datetime.timedelta(days=3).strftime('%d/%m/%Y')
            else:
                return today - datetime.timedelta(days=1).strftime('%d/%m/%Y')
        else:
            return today.strftime('%d/%m/%Y')

ld = get_latest_trading_date()

print(ld)

df = pd.read_excel('W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx', sheet_name='Stock')

hose_df = df.loc[df['exchange'] == 'HOSE']

ticker_df = hose_df['ticker']

for ticker in ticker_df:
    json_data = client.intraday_ohlc(config, model.intraday_ohlc(ticker, ld, ld, 1, 300, True, 1))

    print(json_data)
    
    time.sleep(1)