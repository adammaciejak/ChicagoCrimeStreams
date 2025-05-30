#!/bin/bash

CLUSTER_NAME="broker-1"
BOOTSTRAP_SERVERS="${CLUSTER_NAME}:19092"

echo "Usuwanie istniejących tematów..."
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --delete --topic crimes-input 2>/dev/null || true
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --delete --topic crimes-aggregates 2>/dev/null || true
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --delete --topic crimes-anomalies 2>/dev/null || true

# Resetowanie aplikacji Kafka Streams
docker exec ${CLUSTER_NAME} kafka-streams-application-reset.sh \
  --bootstrap-server ${BOOTSTRAP_SERVERS} \
  --application-id chicago-crimes-app \
  --input-topics crimes-input \
  --to-earliest 2>/dev/null || true

echo "Tworzenie nowych tematów..."
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --create --replication-factor 2 --partitions 3 --topic crimes-input
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --create --replication-factor 2 --partitions 3 --topic crimes-aggregates
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --create --replication-factor 2 --partitions 3 --topic crimes-anomalies

echo "Tematy utworzone pomyślnie:"
docker exec ${CLUSTER_NAME} kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --list
