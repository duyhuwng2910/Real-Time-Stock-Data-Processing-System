from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_topic_name = "demo"
kafka_bootstrap_servers = "kafka-broker-1:9093,kafka-broker-2:9094,kafka-broker-3:9095"

# Create Spark Session
spark_conn = SparkSession.builder.appName("Real Time Stock Data Processing Project") \
    .master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate()

spark_conn.sparkContext.setLogLevel("ERROR")


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

    # Extract topic information and apply suitable schema
    json_df = df.selectExpr("CAST(value as STRING) as value")

    json_schema = StructType([
            StructField("RType", StringType(), True),
            StructField("TradingDate", StringType(), True),
            StructField("Symbol", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField('Close', DoubleType(), True),
            StructField('Volume', DoubleType(), True),
            StructField('Value', DoubleType(), True)
        ])

    stock_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")

    # print(stock_df)
    #
    # stock_df1 = stock_df.select("stock_data.*")
    #
    # print(stock_df1)

    # print the dataframe in console for debug purpose
    streaming_query = stock_df.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .format('console') \
        .option("truncate", "false") \
        .start()

    streaming_query.awaitTermination()

    print("Task completed!")


def main():
    data = {'DataType': 'B',
            'Content': '{"RType":"B",'
                       '"TradingDate":"05/03/2024",'
                       '"Time":"14:45:00",'
                       '"Symbol":"HPG",'
                       '"Open":31150.0,'
                       '"High":31150.0,'
                       '"Low":31150.0,'
                       '"Close":31150.0,'
                       '"Volume":1473600.0,'
                       '"Value":0.0}'}

    run_spark_job()


main()
