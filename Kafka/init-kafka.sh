#!/bin/bash
set -e

/opt/bitnami/scripts/kafka/entrypoint.sh /opt/bitnami/scripts/kafka/run.sh &

echo "Waiting for Kafka to be set up..."
sleep 10

kafka-topics.sh --create --topic hose --bootstrap-server localhost:9092 --partitions 5 --replication-factor 4

kafka-topics.sh --create --topic hnx --bootstrap-server localhost:9092 --partitions 5 --replication-factor 4

kafka-topics.sh --create --topic upcom --bootstrap-server localhost:9092 --partitions 5 --replication-factor 4

wait