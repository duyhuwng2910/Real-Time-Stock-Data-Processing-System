from ssi_fc_data import fc_md_client
import datetime
import sys

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/project/SSI')

import config

client = fc_md_client.MarketDataClient(config)


def get_latest_trading_date():
    """
        Function to return the latest trading date, according the time running this function
    """
    today = datetime.date.today()

    weekday = today.weekday()

    # Nếu là thứ Bảy hoặc Chủ Nhật, trả về ngày thứ Sáu của tuần đó
    if weekday == 5:
        return today - datetime.timedelta(days=1)
    elif weekday == 6:
        return today - datetime.timedelta(days=2)
    else:
        now = datetime.datetime.now()

        if now.hour >= 15 and now.minute >= 30:
            return today
        else:
            if weekday == 0:
                return today - datetime.timedelta(days=3)
            else:
                return today - datetime.timedelta(days=1)


ld = get_latest_trading_date()

print(ld)

# df = pd.read_excel('W:/study/UET/Graduation Thesis/Real-time-stock-data-processing-system/Excel files/vn_stock.xlsx', sheet_name='Stock')
#
# hose_df = df.loc[df['exchange'] == 'HOSE']
#
# ticker_df = hose_df['ticker']
#
# for ticker in ticker_df:
#     json_data = client.intraday_ohlc(config, model.intraday_ohlc(ticker, ld, ld, 1, 300, True, 1))
#
#     print(json_data)
#
#     time.sleep(1)