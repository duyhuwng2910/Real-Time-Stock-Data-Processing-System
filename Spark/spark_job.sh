### Spark

# Truy cập vào cửa sổ dòng lệnh của Spark container
docker exec -it spark-master bash

pip install mysql-connector-python SQLAlchemy py4j

## Spark Structured Streaming
# Thực thi spark structured streaming application
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --master spark://spark-master:7077 consumer.py

# Cho mục đích kiểm thử
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --master spark://spark-master:7077 consumer_test.py

## Spark ML
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --master spark://spark-master:7077 trend_analytic.py

## Cho mục đích kiểm thử
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 Spark/trend_analytic.py