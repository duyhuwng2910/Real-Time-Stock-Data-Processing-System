from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Job").getOrCreate()

df = spark.read.csv("error_companies_overview_list.csv")

df.show()
