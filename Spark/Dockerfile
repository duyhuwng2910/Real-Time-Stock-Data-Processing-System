FROM bitnami/spark

USER root

COPY conf/metrics-graphite.properties /opt/bitnami/spark/conf/metrics.properties

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

ENV SPARK_METRICS_ENABLED=true

ENV SPARK_UI_PROMETHEUS_ENABLED=true