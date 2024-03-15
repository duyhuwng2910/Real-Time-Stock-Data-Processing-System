import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


kafka_topic_name = "hose"
kafka_bootstrap_servers = "kafka-broker-1:9093"

jdbc_url = "jdbc:mysql://mysql:3306/vietnam_stock"

db_credentials = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

def write_to_mysql(df, epoc_id):
    df.write \
        .jdbc(url=jdbc_url,
              table="real_time_stock_trading_data",
              mode="append",
              properties=db_credentials)


if __name__ == "__main__":
    # Create Spark Session
    spark_conn = SparkSession.builder.appName("Real Time Stock Data Processing Demo") \
        .master("local[*]") \
        .config('spark.jars.packages', 'com.mysql:mysql-connector-j:8.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0') \
        .getOrCreate()

    spark_conn.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame to connect Spark Structured Streaming
    # with Kafka topic to read Data Streams
    df = spark_conn \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
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

    stock_df1 = stock_df1.withColumn("trading_time", date_format(stock_df1['trading_time'], "yyyy-MM-dd HH:mm:ss"))

    stock_df1.printSchema()

    stock_df1 = stock_df1.withColumnRenamed("Symbol", "ticker") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Close", "close") \
        .withColumnRenamed("Volume", "volume") \
        .withColumnRenamed("offset", "id")

    stock_df2 = stock_df1.select('id', 'trading_time', 'ticker', 'open', 'high', 'low', 'close', 'volume')

    query = stock_df2.writeStream \
        .foreachBatch(write_to_mysql) \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

    print("Task completed!")