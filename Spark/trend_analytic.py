from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pandas as ps

from cassandra.cluster import Cluster

# Create Spark Session
spark_conn = SparkSession.builder \
    .appName("Real Time Stock Data Analytic Project") \
    .config("spark.cassandra.connection.host", "cassandra-1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

spark_conn.sparkContext.setLogLevel("ERROR")

# Create cluster
cluster = Cluster(['localhost'], port=9042)

session = cluster.connect()

session.set_keyspace("vietnam_stock")


def pandas_factory(colnames, rows):
    """
        Function to get pyspark pandas dataframe
        from a query in Cassandra table
    """
    return ps.DataFrame(rows, columns=colnames)


session.row_factory = pandas_factory

session.default_fetch_size = 10000000


def main():
    dataset = session.execute("""
        SELECT * FROM stock_data_for_ml
        WHERE ticker = 'SSI'
    """)

    ps_df = dataset._current_rows

    ps_df['trading_date'] = ps_df['trading_time'].dt.date

    ps_df['time'] = ps_df['trading_time'].dt.strftime('%H:%M:%S').astype('str')

    ps_df1 = ps_df[
        ['trading_date', 'time', 'ticker', 'price', 'volume',
         'last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
         'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
         'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
         'last_five_minutes_volume', 'next_one_minute_price', 'next_five_minutes_price'
         ]
    ]

    # print(ps_df1.dtypes)
    #
    # print(ps_df1.head())


if __name__ == "__main__":
    main()
