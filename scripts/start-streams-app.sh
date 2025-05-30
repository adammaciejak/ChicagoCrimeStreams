#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $0 <anomaly-days> <anomaly-percentage> <delay-mode[A|C]>"
    echo "Example: $0 7 40 A"
    exit 1
fi

ANOMALY_DAYS=$1
ANOMALY_PERCENTAGE=$2
DELAY_MODE=$3
CLUSTER_NAME="broker-1"
BOOTSTRAP_SERVERS="${CLUSTER_NAME}:19092"

echo "Uruchamianie aplikacji Kafka Streams..."
echo "Parametry anomalii: $ANOMALY_DAYS dni, $ANOMALY_PERCENTAGE%"
echo "Tryb opóźnień: $DELAY_MODE"

# Skopiuj JAR do kontenera
docker cp target/chicago-crimes-streams.jar ${CLUSTER_NAME}:/tmp/

# Uruchom aplikację
docker exec -it ${CLUSTER_NAME} java -cp /opt/kafka/libs/*:/tmp/chicago-crimes-streams.jar \
    com.chicago.crimes.ChicagoCrimesStreamsApp \
    ${BOOTSTRAP_SERVERS} \
    ${ANOMALY_DAYS} \
    ${ANOMALY_PERCENTAGE} \
    ${DELAY_MODE}
