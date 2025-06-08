#!/bin/bash

echo "=== RESETOWANIE ŚRODOWISKA ==="

# Zatrzymaj aplikacje
pkill -f "chicago-crimes-app" 2>/dev/null
pkill -f "CrimesDataProducer" 2>/dev/null
pkill -f "connect-standalone" 2>/dev/null

# Resetuj Kafka Streams
./kafka-streams-application-reset.sh \
    --bootstrap-server broker-1:19092 \
    --application-id chicago-crimes-app \
    --input-topics crimes-input \
    --to-earliest 2>/dev/null

# Usuń tematy
./kafka-topics.sh --bootstrap-server broker-1:19092 --delete --topic crimes-input 2>/dev/null
./kafka-topics.sh --bootstrap-server broker-1:19092 --delete --topic crimes-aggregates 2>/dev/null
./kafka-topics.sh --bootstrap-server broker-1:19092 --delete --topic crimes-anomalies 2>/dev/null

# Czekaj na usunięcie
sleep 5

# Utwórz nowe tematy
./kafka-topics.sh --bootstrap-server broker-1:19092 --create \
    --replication-factor 2 --partitions 3 --topic crimes-input

./kafka-topics.sh --bootstrap-server broker-1:19092 --create \
    --replication-factor 2 --partitions 3 --topic crimes-aggregates

./kafka-topics.sh --bootstrap-server broker-1:19092 --create \
    --replication-factor 2 --partitions 3 --topic crimes-anomalies

# Wyczyść stan lokalny
rm -rf /tmp/kafka-streams* 2>/dev/null
rm -rf /tmp/chicago-crimes-app* 2>/dev/null

echo "✓ Środowisko zresetowane"
