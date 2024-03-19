import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_topic_name = "hose"
kafka_bootstrap_servers = "kafka-broker-1:9093,kafka-broker-2:9094,kafka-broker-3:9095"

# Create Spark Session
spark_conn = SparkSession.builder \
    .appName("Real Time Stock Data Processing Project") \
    .config("spark.cassandra.connection.host", "cassandra-1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config('spark.jars.packages', 'com.mysql:mysql-connector-j:8.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0') \
    .master("local[*]") \
    .getOrCreate()

spark_conn.sparkContext.setLogLevel("ERROR")


def write_to_aggregation_table(df, epoc_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="test", keyspace="vietnam_stock") \
            .mode("append") \
            .save()
            
        print("Ok")
    except Exception as e:
        print(f"Error while writing to Cassandra:{e}")


if __name__ == "__main__":
    # Construct a streaming DataFrame to connect Spark Structured Streaming
    # with Kafka topic to read Data Streams
    df = spark_conn \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Schema of the dataframe: ")

    df.printSchema()

    time.sleep(1)

    json_df = df.selectExpr("offset", "CAST(value AS STRING)")

    print("Schema of the json dataframe: ")

    json_df.printSchema()
    
    time.sleep(1)
    
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

    stock_df = json_df.select(json_df['offset'], from_json(col("value"), json_schema).alias("data")).selectExpr("offset", "data.*")

    stock_df1 = stock_df.withColumn("TradingDate", to_date(stock_df['TradingDate'], "dd/MM/yyyy"))

    stock_df1 = stock_df1.withColumn("trading_time",
                                     concat_ws(" ", stock_df1['TradingDate'], stock_df['Time'])
                                     )

    stock_df1 = stock_df1.withColumn("trading_time", col("trading_time").cast(TimestampType()))

    # stock_df1 = stock_df1.withColumn("trading_time", date_format(stock_df1['trading_time'], "yyyy-MM-dd HH:mm:ss"))

    stock_df1 = stock_df1.withColumnRenamed("Symbol", "ticker") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Close", "close") \
        .withColumnRenamed("Volume", "volume") \
        .withColumnRenamed("offset", "id")
    
    time.sleep(1)
    
    print("Schema of stock data frame:")
    
    stock_df1.printSchema()

    aggregation_df = stock_df1.withColumn("trading_time", date_format("trading_time", "yyyy-MM-dd HH:mm:00"))
    
    aggregation_df = aggregation_df.withColumn("trading_time", to_timestamp("trading_time", "yyyy-MM-dd HH:mm:ss"))
    
    time.sleep(1)
    
    print("Schema of aggregation data frame:")
    
    aggregation_df.printSchema()
    
    aggregation_df = aggregation_df \
        .withWatermark("trading_time", "3 minutes") \
        .groupBy(
            col("ticker"), 
            window("trading_time", "1 minute", "1 minute")) \
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
    
    data = aggregation_df1.writeStream \
        .foreachBatch(write_to_aggregation_table) \
        .outputMode("update") \
        .start()

    data.awaitTermination()
    
    print("Task completed!")