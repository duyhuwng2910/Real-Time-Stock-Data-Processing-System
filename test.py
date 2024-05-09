import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("linear regression test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ml_df = spark.read.csv("stock_data.csv", header=True, inferSchema=True)

ml_df = ml_df.select("trading_time", "ticker", "price", "volume",
                     "last_one_minute_price", "last_one_minute_volume",
                     "last_two_minutes_price", "last_two_minutes_volume",
                     "last_three_minutes_price", "last_three_minutes_volume",
                     "last_four_minutes_price", "last_four_minutes_volume",
                     "last_five_minutes_price", "last_five_minutes_volume")

stream_df = spark.read.csv("intraday.csv", header=True, inferSchema=True)

stream_df = stream_df.select("trading_time", "ticker", "price", "volume",
                             "last_one_minute_price", "last_one_minute_volume",
                             "last_two_minutes_price", "last_two_minutes_volume",
                             "last_three_minutes_price", "last_three_minutes_volume",
                             "last_four_minutes_price", "last_four_minutes_volume",
                             "last_five_minutes_price", "last_five_minutes_volume")

window = Window.partitionBy("ticker").orderBy(desc("trading_time"))

stream_df = stream_df.withColumn("row_number", row_number().over(window))

stream_df = stream_df.where(col("row_number") == 1)

stream_df = stream_df \
    .withColumn("last_one_minute_price",
                when(col("row_number") == 1, lag("price", -1).over(window))
                .otherwise(col("last_one_minute_price"))) \
    .withColumn("last_one_minute_volume",
                when(col("row_number") == 1, lag("volume", -1).over(window))
                .otherwise(col("last_one_minute_volume"))) \
    .withColumn("last_two_minutes_price",
                when(col("row_number") == 1, lag("price", -2).over(window))
                .otherwise(col("last_two_minutes_price"))) \
    .withColumn("last_two_minutes_volume",
                when(col("row_number") == 1, lag("volume", -2).over(window))
                .otherwise(col("last_two_minutes_volume"))) \
    .withColumn("last_three_minutes_price",
                when(col("row_number") == 1, lag("price", -3).over(window))
                .otherwise(col("last_three_minutes_price"))) \
    .withColumn("last_three_minutes_volume",
                when(col("row_number") == 1, lag("volume", -3).over(window))
                .otherwise(col("last_three_minutes_volume"))) \
    .withColumn("last_four_minutes_price",
                when(col("row_number") == 1, lag("price", -4).over(window))
                .otherwise(col("last_four_minutes_price"))) \
    .withColumn("last_four_minutes_volume",
                when(col("row_number") == 1, lag("volume", -4).over(window))
                .otherwise(col("last_four_minutes_volume"))) \
    .withColumn("last_five_minutes_price",
                when(col("row_number") == 1, lag("price", -5).over(window))
                .otherwise(col("last_five_minutes_price"))) \
    .withColumn("last_five_minutes_volume",
                when(col("row_number") == 1, lag("volume", -5).over(window))
                .otherwise(col("last_five_minutes_volume"))) \
    .orderBy("ticker", "trading_time")

df1.show()

spark.stop()
