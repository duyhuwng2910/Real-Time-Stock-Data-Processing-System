FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip

RUN pip install pandas cassandra-driver pyarrow

COPY conf/metrics-graphite.properties /opt/bitnami/spark/conf/metrics.properties

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

ENV SPARK_METRICS_ENABLED=true

ENV SPARK_UI_PROMETHEUS_ENABLED=true