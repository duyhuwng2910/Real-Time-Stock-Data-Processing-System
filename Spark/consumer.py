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
    .master("local[*]") \
    .getOrCreate()

spark_conn.sparkContext.setLogLevel("ERROR")


def write_to_cassandra(df, epochId):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="real_time_stock_trading_data", keyspace="vietnam_stock") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to Cassandra:{e}")


def run_spark_job():
    df = spark_conn \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Schema of the dataframe: ")

    df.printSchema()

    json_df = df.selectExpr("CAST(value AS STRING)")

    print("Schema of the dataframe: ")

    json_df.printSchema()

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

    stock_df = json_df.select(from_json(col("value"), json_schema).alias("data")).selectExpr("data.*")

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
        .withColumnRenamed("Volume", "volume")

    stock_df2 = stock_df1.select('trading_time', 'ticker', 'open', 'high', 'low', 'close', 'volume')

    # print the dataframe in console for debug purpose
    # streaming_query = stock_df2.writeStream \
    #     .trigger(processingTime='10 seconds') \
    #     .outputMode("append") \
    #     .format('console') \
    #     .option("truncate", "false") \
    #     .start()
    #
    # streaming_query.awaitTermination()

    cassandra_table = stock_df2.writeStream \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .foreachBatch(write_to_cassandra) \
        .start()

    cassandra_table.awaitTermination()

    print("Task completed!")


def main():
    run_spark_job()


main()
