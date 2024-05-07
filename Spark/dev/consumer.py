import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegressionModel

kafka_topic_name = "stock"
kafka_bootstrap_servers = "kafka-broker-1:9093,kafka-broker-2:9094"

# Create Spark Session
spark_conn = SparkSession.builder \
    .appName("Real Time Stock Data Processing Project") \
    .config("spark.cassandra.connection.host", "cassandra-1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark_conn.sparkContext.setLogLevel("ERROR")

trend_analysis_df = spark_conn.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stock_trend_analysis_data", keyspace="vietnam_stock") \
    .load()

trend_analysis_df = trend_analysis_df.select("trading_time", "ticker", "price", "volume",
                                             "last_one_minute_price", "last_one_minute_volume",
                                             "last_two_minutes_price", "last_two_minutes_volume",
                                             "last_three_minutes_price", "last_three_minutes_volume",
                                             "last_four_minutes_price", "last_four_minutes_volume",
                                             "last_five_minutes_price", "last_five_minutes_volume")

column_list = ['last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
               'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
               'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
               'last_five_minutes_volume']

one_minute_model = LinearRegressionModel.read().load("one_minute_model/")

five_minutes_model = LinearRegressionModel.read().load("five_minutes_model/")


def process_real_time_stock_trading_data(json_df):
    json_schema = (StructType()
                   .add("RType", StringType(), False)
                   .add("TradingDate", StringType(), False)
                   .add("Time", StringType(), False)
                   .add("Symbol", StringType(), False)
                   .add("Open", DoubleType(), False)
                   .add("High", DoubleType(), False)
                   .add("Low", DoubleType(), False)
                   .add("Close", DoubleType(), False)
                   .add("Volume", DoubleType(), True)
                   .add("Value", DoubleType(), False))

    stock_df = json_df.select(json_df['offset'], from_json(col("value"), json_schema).alias("data")).selectExpr(
        "offset", "data.*")

    stock_df = stock_df.withColumn("TradingDate", to_date(stock_df['TradingDate'], "dd/MM/yyyy"))

    stock_df = stock_df.withColumn("trading_time",
                                   concat_ws(" ", stock_df['TradingDate'], stock_df['Time'])
                                   )

    stock_df = stock_df.withColumn("trading_time", col("trading_time").cast(TimestampType()))

    stock_df = stock_df.withColumn("trading_time", date_format(stock_df['trading_time'], "yyyy-MM-dd HH:mm:ss"))

    stock_df = stock_df.withColumnRenamed("Symbol", "ticker") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Close", "close") \
        .withColumnRenamed("Volume", "volume") \
        .withColumnRenamed("offset", "id")

    return stock_df


def run_aggregation_task(aggregation_df):
    aggregation_df = aggregation_df \
        .withWatermark("trading_time", "5 minutes") \
        .groupBy(
        col("ticker"),
        window("trading_time", "1 minute", "1 minute")
    ) \
        .agg(
        first("open").alias("open"),
        max("high").alias("high"),
        min("low").alias("low"),
        last("close").alias("close"),
        sum("volume").alias("volume")
    )

    aggregation_df1 = aggregation_df.select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        'ticker',
        'open',
        'high',
        'low',
        'close',
        'volume'
    )

    return aggregation_df1


def write_to_real_time_table(df, epoc_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="real_time_stock_trading_data", keyspace="vietnam_stock") \
            .mode("append") \
            .save()

        print("Write real time stock trading to Cassandra table successfully!")
    except Exception as e:
        print(f"Error while writing to Cassandra:{e}")


def write_to_aggregation_table(df, epoc_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="aggregated_stock_trading_data", keyspace="vietnam_stock") \
            .mode("append") \
            .save()

        print("Aggregate stock data successfully!")
    except Exception as e:
        print(f"Error while aggregating to Cassandra:{e}")


def write_to_trend_analysis_table(df, epoc_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="stock_trend_analysis_data", keyspace="vietnam_stock") \
            .mode("append") \
            .save()

        print("Analyze trend of stock price successfully!")
    except Exception as e:
        print(f"Error while analyzing to Cassandra:{e}")


def run_spark_job():
    df = spark_conn \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Schema of the dataframe: ")

    df.printSchema()

    json_df = df.selectExpr("offset", "CAST(value AS STRING)")

    time.sleep(1)

    print("Schema of the json dataframe: ")

    json_df.printSchema()

    stock_df = process_real_time_stock_trading_data(json_df)

    real_time_stock_df = stock_df.select('id', 'trading_time', 'ticker', 'open', 'high', 'low', 'close', 'volume')

    aggregation_df = stock_df.withColumn("trading_time", date_format("trading_time", "yyyy-MM-dd HH:mm:00"))

    aggregation_df = aggregation_df.withColumn("trading_time", to_timestamp("trading_time", "yyyy-MM-dd HH:mm:ss"))

    time.sleep(1)

    print("Schema of aggregation data frame:")

    aggregation_df.printSchema()

    aggregation_df1 = run_aggregation_task(aggregation_df)

    prediction_df = aggregation_df1.select("end_time", "ticker", "close", "volume")

    prediction_df = prediction_df.withColumnRenamed("end_time", "trading_time") \
        .withColumnRenamed("close", "price")

    for column in column_list:
        prediction_df = prediction_df.withColumn(column, lit(0))

    prediction_df = trend_analysis_df.union(prediction_df)

    time.sleep(1)

    print("Schema of prediction data frame:")

    prediction_df.printSchema()

    window = Window.partitionBy("ticker").orderBy(desc("trading_time"))

    prediction_df = prediction_df.withColumn("row_number", row_number().over(window)) \
        .orderBy("ticker", "trading_time")

    prediction_df1 = prediction_df \
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

    prediction_df = prediction_df.withColumn("hour", hour(col("trading_time"))) \
        .withColumn("minute", minute(col("trading_time")))

    time.sleep(1)

    print("Schema of new prediction data frame:")

    prediction_df.printSchema()

    # Create StringIndexer for column 'ticker'
    string_indexer = StringIndexer(inputCol="ticker", outputCol="ticker_index")

    prediction_df1 = string_indexer.fit(prediction_df1).transform(prediction_df1)

    onehot_encoder = OneHotEncoder(inputCols=["ticker_index"], outputCols=["ticker_encoded"])

    prediction_df1 = onehot_encoder.fit(prediction_df1).transform(prediction_df1)

    prediction_df2 = prediction_df1.select("hour", "minute", "price", "ticker_encoded", "volume",
                                           "last_one_minute_price", "last_one_minute_volume",
                                           "last_two_minutes_price", "last_two_minutes_volume",
                                           "last_three_minutes_price", "last_three_minutes_volume",
                                           "last_four_minutes_price", "last_four_minutes_volume",
                                           "last_five_minutes_price", "last_five_minutes_volume") \
        .orderBy(desc("trading_time")) \
        .limit(1)

    time.sleep(1)

    print("Schema of new prediction data frame:")

    prediction_df2.printSchema()

    prediction_df3 = one_minute_model.transform(prediction_df2)

    prediction_df4 = five_minutes_model.transform(prediction_df2)

    prediction_df3 = prediction_df3.withColumnRenamed("prediction", "next_one_minute_price")

    prediction_df4 = prediction_df4.withColumnRenamed("prediction", "next_five_minutes_price")

    prediction_df5 = prediction_df3.join(prediction_df4, on=[
        prediction_df3.col("trading_time") == prediction_df4.col("trading_time"),
        prediction_df3.col("ticker") == prediction_df4.col("ticker")
    ], how="inner")

    prediction_df6 = prediction_df5.select("trading_time", "price", "ticker", "volume",
                                           "last_one_minute_price", "last_one_minute_volume",
                                           "last_two_minutes_price", "last_two_minutes_volume",
                                           "last_three_minutes_price", "last_three_minutes_volume",
                                           "last_four_minutes_price", "last_four_minutes_volume",
                                           "last_five_minutes_price", "last_five_minutes_volume",
                                           "next_one_minute_price", "next_five_minutes_price")

    time.sleep(1)

    print("Schema of final prediction data frame:")

    prediction_df6.printSchema()

    real_time_table = real_time_stock_df.writeStream \
        .trigger(processingTime="3 seconds") \
        .outputMode("append") \
        .foreachBatch(write_to_real_time_table) \
        .start()

    aggregation_table = aggregation_df1.writeStream \
        .foreachBatch(write_to_aggregation_table) \
        .outputMode("update") \
        .start()

    trend_analysis_table = prediction_df6.writeStream \
        .foreachBatch(write_to_trend_analysis_table) \
        .outputMode("update") \
        .start()

    real_time_table.awaitTermination()

    aggregation_table.awaitTermination()

    trend_analysis_table.awaitTermination()

    spark_conn.stop()


def main():
    run_spark_job()


main()
