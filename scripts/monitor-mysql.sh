#!/bin/bash

echo "=== MONITOROWANIE MYSQL ==="

docker exec -i chicago-mysql mysql -u chicago_user -pchicago_pass chicago_crimes << 'SQL'
-- Statystyki tabel
SELECT 'Aggregates' as table_name, COUNT(*) as record_count FROM `crime_aggregates`
UNION ALL
SELECT 'Anomalies' as table_name, COUNT(*) as record_count FROM `crime_anomalies`;

-- Najnowsze agregacje
SELECT *
FROM `crime_aggregates`
ORDER BY `created_at` DESC
LIMIT 5;

-- Najnowsze anomalie
SELECT *
FROM `crime_anomalies`
ORDER BY `detected_at` DESC
LIMIT 5;
SQL
