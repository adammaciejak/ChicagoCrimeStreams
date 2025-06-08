#!/bin/bash

echo "=== KONFIGURACJA MYSQL ==="

# Zatrzymaj stary kontener
docker stop chicago-mysql 2>/dev/null
docker rm chicago-mysql 2>/dev/null

# Uruchom MySQL
docker exec -it broker-1 mkdir -p /tmp/mysql-datadir
docker run --name chicago-mysql \
    -v /tmp/mysql-datadir:/var/lib/mysql \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=chicago-secret \
    --network $(docker network ls --filter name=kafka --format "{{.Name}}") \
    -d mysql:8.0

echo "Czekanie na uruchomienie MySQL..."
sleep 60

# Konfiguruj bazę danych
docker exec -i chicago-mysql mysql -uroot -pchicago-secret << 'SQL'
CREATE USER 'chicago_user'@'%' IDENTIFIED BY 'chicago_pass';
CREATE DATABASE IF NOT EXISTS chicago_crimes CHARACTER SET utf8;
GRANT ALL ON chicago_crimes.* TO 'chicago_user'@'%';
FLUSH PRIVILEGES;
SQL

# Utwórz tabele
docker exec -i chicago-mysql mysql -u chicago_user -pchicago_pass chicago_crimes << 'SQL'
CREATE TABLE IF NOT EXISTS `crime_aggregates` (
    `id` VARCHAR(255) PRIMARY KEY,
    `year_month` VARCHAR(7) NOT NULL,
    `primary_description` VARCHAR(100) NOT NULL,
    `district` VARCHAR(10) NOT NULL,
    `total_crimes` INT NOT NULL,
    `arrest_count` INT NOT NULL,
    `domestic_count` INT NOT NULL,
    `fbi_index_count` INT NOT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `crime_anomalies` (
    `id` VARCHAR(255) PRIMARY KEY,
    `window_start` VARCHAR(30) NOT NULL,
    `window_end` VARCHAR(30) NOT NULL,
    `district` VARCHAR(10) NOT NULL,
    `fbi_index_crimes` BIGINT NOT NULL,
    `total_crimes` BIGINT NOT NULL,
    `fbi_percentage` DECIMAL(5,2) NOT NULL,
    `detected_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SQL

echo "✓ MySQL skonfigurowany"
