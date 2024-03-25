FROM bitnami/spark
USER root

# COPY conf/metrics-prometheus.properties /opt/bitnami/spark/conf/metrics.properties

COPY conf/metrics-graphite.properties /opt/bitnami/spark/conf/metrics.properties

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

COPY consumer.py /opt/bitnami/spark/consumer.py

COPY dev/consumer.py /opt/bitnami/spark/consumer_test.py

ENV SPARK_METRICS_ENABLED=true

ENV SPARK_UI_PROMETHEUS_ENABLED=true