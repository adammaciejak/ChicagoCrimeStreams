#!/bin/bash

echo "=== URUCHAMIANIE KAFKA STREAMS ==="

if [ $# -lt 3 ]; then
    echo "Użycie: $0 <dni-anomalii> <procent-anomalii> <tryb-delay>"
    echo "Przykład: $0 7 60 A"
    exit 1
fi

ANOMALY_DAYS=$1
ANOMALY_PERCENTAGE=$2
DELAY_MODE=$3
IUCR_FILE="/tmp/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv"

echo "Parametry: $ANOMALY_DAYS dni, $ANOMALY_PERCENTAGE%, tryb $DELAY_MODE"

# Sprawdź pliki
if [ ! -f "/tmp/chicago-crimes-streams.jar" ]; then
    echo "BŁĄD: Brak pliku chicago-crimes-streams.jar"
    exit 1
fi

if [ ! -f "$IUCR_FILE" ]; then
    echo "BŁĄD: Brak pliku IUCR"
    exit 1
fi

# Uruchom aplikację
java -cp /opt/kafka/libs/*:/tmp/chicago-crimes-streams.jar \
    com.chicago.crimes.ChicagoCrimesStreamsApp \
    broker-1:19092 \
    "$ANOMALY_DAYS" \
    "$ANOMALY_PERCENTAGE" \
    "$DELAY_MODE" \
    "$IUCR_FILE"
