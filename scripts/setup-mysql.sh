#!/bin/bash

CLUSTER_NAME="broker-1"

echo "Konfigurowanie MySQL jako ujście danych..."

# Tworzenie katalogu dla MySQL
docker exec ${CLUSTER_NAME} mkdir -p /tmp/mysql_data

# Uruchomienie kontenera MySQL
docker exec ${CLUSTER_NAME} docker run --name crimes-mysql \
    -v /tmp/mysql_data:/var/lib/mysql \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=password123 \
    -e MYSQL_DATABASE=crimes_db \
    -e MYSQL_USER=crimes_user \
    -e MYSQL_PASSWORD=crimes_pass \
    -d mysql:8.0

echo "Oczekiwanie na uruchomienie MySQL..."
sleep 30

# Tworzenie tabel
docker exec ${CLUSTER_NAME} docker exec crimes-mysql mysql -u crimes_user -pcrimes_pass crimes_db << 'EOF'
CREATE TABLE IF NOT EXISTS crime_aggregates (
    id VARCHAR(255) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    primary_description VARCHAR(100) NOT NULL,
    district VARCHAR(10) NOT NULL,
    total_crimes BIGINT DEFAULT 0,
    arrest_count BIGINT DEFAULT 0,
    domestic_count BIGINT DEFAULT 0,
    fbi_index_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start VARCHAR(30) NOT NULL,
    window_end VARCHAR(30) NOT NULL,
    district VARCHAR(10) NOT NULL,
    fbi_index_crimes BIGINT NOT NULL,
    total_crimes BIGINT NOT NULL,
    fbi_percentage DECIMAL(5,2) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF

echo "MySQL skonfigurowany pomyślnie!"
