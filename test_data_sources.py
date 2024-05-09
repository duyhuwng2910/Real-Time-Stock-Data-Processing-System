import datetime
import time
import sys

import vnstock_data
import vnstock
import pandas as pd
import pyspark.pandas as pspd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

# spark = SparkSession.builder.appName("test").getOrCreate()
#
# spark.sparkContext.setLogLevel("ERROR")

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

# ticker_df = vnstock_data.ssi.get_index_component(client, config, index='VN30', page=1, pageSize=100)

today = datetime.date.today()

start_date = today - datetime.timedelta(days=90)

intraday_data = vnstock_data.stock_historical_data(symbol='SSI',
                                                   start_date=str(start_date),
                                                   end_date=str(today),
                                                   resolution='1',
                                                   type='stock',
                                                   beautify=True,
                                                   decor=False,
                                                   source='DNSE')

print(intraday_data.head(1))
