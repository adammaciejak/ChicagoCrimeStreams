#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <csv-file-path> <records-per-second>"
    exit 1
fi

CSV_FILE=$1
RECORDS_PER_SECOND=$2
CLUSTER_NAME="broker-1"
BOOTSTRAP_SERVERS="${CLUSTER_NAME}:19092"

echo "Starting crimes data producer..."
echo "CSV file: $CSV_FILE"
echo "Records per second: $RECORDS_PER_SECOND"

# Skopiuj plik JAR do kontenera jeśli jeszcze nie istnieje
docker cp target/chicago-crimes-streams.jar ${CLUSTER_NAME}:/tmp/ 2>/dev/null || true

# Uruchom producera w kontenerze
docker exec -it ${CLUSTER_NAME} java -cp /opt/kafka/libs/*:/tmp/chicago-crimes-streams.jar \
    com.chicago.crimes.producer.CrimesDataProducer \
    ${BOOTSTRAP_SERVERS} \
    ${CSV_FILE} \
    ${RECORDS_PER_SECOND}
