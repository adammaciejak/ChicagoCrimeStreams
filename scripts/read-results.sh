#!/bin/bash

CLUSTER_NAME="broker-1"
BOOTSTRAP_SERVERS="${CLUSTER_NAME}:19092"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <topic> [from-beginning]"
    echo "Available topics: crimes-aggregates, crimes-anomalies"
    exit 1
fi

TOPIC=$1
FROM_BEGINNING=""

if [ "$2" = "from-beginning" ]; then
    FROM_BEGINNING="--from-beginning"
fi

echo "Odczytywanie wyników z tematu: $TOPIC"

docker exec -it ${CLUSTER_NAME} kafka-console-consumer.sh \
    --bootstrap-server ${BOOTSTRAP_SERVERS} \
    --topic ${TOPIC} \
    --property print.key=true \
    --property print.value=true \
    --property key.separator=" => " \
    ${FROM_BEGINNING}
