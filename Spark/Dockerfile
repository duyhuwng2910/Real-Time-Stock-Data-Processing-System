FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip

RUN pip install pandas cassandra-driver pyarrow

#RUN install_packages curl
#
#RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar --output /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar
#
#RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.1/spark-streaming-kafka-0-10_2.12-3.5.1.jar --output /opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.1.jar
#
#RUN curl https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar --output /opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.5.0.jar

COPY conf/metrics-graphite.properties /opt/bitnami/spark/conf/metrics.properties

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

ENV SPARK_METRICS_ENABLED=true

ENV SPARK_UI_PROMETHEUS_ENABLED=true