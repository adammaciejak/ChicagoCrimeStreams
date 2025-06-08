#!/bin/bash

echo "=== KONFIGURACJA KAFKA CONNECT ==="

# Pobierz sterowniki
wget -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar 2>/dev/null
cp mysql-connector-j-8.0.33.jar /opt/kafka/libs/ 2>/dev/null

wget -q https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.7.0/kafka-connect-jdbc-10.7.0.jar 2>/dev/null
mkdir -p /opt/kafka/plugin
cp kafka-connect-jdbc-10.7.0.jar /opt/kafka/plugin/ 2>/dev/null

# Konfiguracja worker
cat > /tmp/connect-standalone.properties << 'EOF'
plugin.path=/opt/kafka/plugin

bootstrap.servers=broker-1:19092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
EOF

# Konfiguracja connectora dla agregatów
cat > /tmp/connect-aggregates-sink.properties << 'EOF'
name=chicago-aggregates-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=crimes-aggregates

connection.url=jdbc:mysql://chicago-mysql:3306/chicago_crimes
connection.user=chicago_user
connection.password=chicago_pass

table.name.format=crime_aggregates
delete.enabled=false
pk.mode=record_key
pk.fields=id
auto.create=false
auto.evolve=false
insert.mode=upsert
EOF

# Konfiguracja connectora dla anomalii
cat > /tmp/connect-anomalies-sink.properties << 'EOF'
name=chicago-anomalies-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=crimes-anomalies

connection.url=jdbc:mysql://chicago-mysql:3306/chicago_crimes
connection.user=chicago_user
connection.password=chicago_pass

table.name.format=crime_anomalies
delete.enabled=false
pk.mode=record_key
pk.fields=id
auto.create=false
auto.evolve=false
insert.mode=upsert
EOF

# Konfiguracja log4j
cp /opt/kafka/config/tools-log4j.properties /opt/kafka/config/connect-log4j.properties
echo "log4j.logger.org.reflections=ERROR" >> /opt/kafka/config/connect-log4j.properties

echo "Uruchamianie Kafka Connect z dwoma connectorami..."

# Uruchom jedną instancję z dwoma connectorami
./connect-standalone.sh \
    /tmp/connect-standalone.properties \
    /tmp/connect-aggregates-sink.properties \
    /tmp/connect-anomalies-sink.properties
