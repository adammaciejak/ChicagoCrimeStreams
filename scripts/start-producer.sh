#!/bin/bash

echo "=== URUCHAMIANIE PRODUCENTA ==="

if [ $# -lt 2 ]; then
    echo "Użycie: $0 <folder-danych> <rekordy-na-sekundę>"
    echo "Przykład: $0 /tmp/crimes-in-chicago_result/ 100"
    exit 1
fi

DATA_FOLDER=$1
RECORDS_PER_SECOND=$2

echo "Folder: $DATA_FOLDER, Prędkość: $RECORDS_PER_SECOND rekordów/s"

# Sprawdź pliki
if [ ! -d "$DATA_FOLDER" ]; then
    echo "BŁĄD: Folder $DATA_FOLDER nie istnieje"
    exit 1
fi

if [ ! -f "/tmp/chicago-crimes-streams.jar" ]; then
    echo "BŁĄD: Brak pliku chicago-crimes-streams.jar"
    exit 1
fi

# Uruchom producenta
java -cp /opt/kafka/libs/*:/tmp/chicago-crimes-streams.jar \
    com.chicago.crimes.producer.CrimesDataProducer \
    broker-1:19092 \
    "$DATA_FOLDER" \
    "$RECORDS_PER_SECOND"
