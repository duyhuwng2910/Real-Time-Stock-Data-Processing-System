import datetime
import time
import sys

import vnstock_data
import vnstock

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Uncomment if you use Windows
# sys.path.append(r'W:/Study/UET/Graduation Thesis/Real-time-stock-data-processing-system/SSI')

# Uncomment if you use Ubuntu
sys.path.append(r'/home/nguyenduyhung/graduation_thesis/Real-Time-Stock-Data-Processing-System/SSI')

import config

client = vnstock_data.ssi.fc_md_client.MarketDataClient(config)

stream_df = spark.read.csv("intraday.csv", header=True, inferSchema=True)

ml_df = spark.read.csv("stock_data.csv", header=True, inferSchema=True).limit(5)

columns = [
            'last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
            'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
            'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
            'last_five_minutes_volume', 'next_one_minute_price', 'next_five_minutes_price'
         ]

for column in columns:
    stream_df = stream_df.withColumn(column, lit(0))

combined_df = ml_df.union(stream_df)

print(combined_df.head(5))

spark.stop()
