#!/bin/bash
set -e

/opt/kafka/entrypoint.sh /opt/bitnami/scripts/kafka/run.sh &

echo "Waiting for Kafka to be set up..."
sleep 10

kafka-topics --create --topic hose --bootstrap-server kafka-controller-1:9092 --replication-factor 2 --partitions 2

kafka-topics --create --topic hose --bootstrap-server kafka-controller-1:9092 --replication-factor 2 --partitions 2

kafka-topics --create --topic hose --bootstrap-server kafka-controller-1:9092 --replication-factor 2 --partitions 2

wait