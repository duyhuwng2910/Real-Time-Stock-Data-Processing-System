from pyspark.sql import SparkSession

spark = SparkSession.builder \
                        .appName("PySpark Job") \
                        .getOrCreate()

df = spark.read.csv("test.csv")

df.show()

spark.stop()
