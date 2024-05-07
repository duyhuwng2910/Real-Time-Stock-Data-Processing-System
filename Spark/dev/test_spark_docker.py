from pyspark.sql import SparkSession

spark = SparkSession.builder \
                        .appName("PySpark Job") \
                        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    # df = spark.read.csv("test.csv", header=True, inferSchema=True)
    #
    # df.show()

    trend_analysis_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="stock_trend_analysis_data", keyspace="vietnam_stock") \
        .load()

    trend_analysis_df.show(5)

    spark.stop()
