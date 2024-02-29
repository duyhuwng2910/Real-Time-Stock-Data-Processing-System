from vnstock_data.ssi import *
import sys
sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI') # Thay đổi đường dẫn tới thư mục chứa file config.py của bạn tại đây. Mẫu file config có trong thư mục docs của repo
import config

client = fc_md_client.MarketDataClient(config)

start_market_data_stream(config, channel='B:VIC')