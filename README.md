# Chicago Crimes Stream Processing - Instrukcja uruchomienia

## Wymagania
- Docker z uruchomionym klastrem Kafka
- Zbudowana aplikacja (plik JAR)
- Pliki z danymi o przestępstwach

## Przygotowanie Dockera
#### 1. Usuń Docker jeśli istnieje
```shell
docker compose down
```

#### 2. Postaw Docker
```shell
cd ApacheKafkaDocker
docker compose up -d
```

#### 3. Zainstaluj biblioteki
```shell
docker exec -u 0 -it broker-1 /bin/bash
apk add libstdc++
exit
```

## Przygotowanie plików

### 1. Skopiuj pliki do kontenera broker-1
#### Zbuduj aplikację
``mvn clean package``
#### Skopiuj pliki

```shell
docker cp target/chicago-crimes-streams.jar broker-1:/tmp/
docker cp Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv broker-1:/tmp/
docker cp /ścieżka/do/danych/crimes-in-chicago_result broker-1:/tmp/
docker cp scripts/ broker-1:/home/appuser/
```

### 2. Wejdź do kontenera
```shell
docker exec -u 0 --workdir /home/appuser -it broker-1 bash
chmod +x scripts/*.sh
```

## Uruchomienie systemu

### Krok 1: Resetuj środowisko
```shell
cd /opt/kafka/bin/
../../../home/appuser/scripts/reset-environment.sh
exit
```

### ### Krok 2: Skonfiguruj MySQL (w folderze z projektem)
```shell
./scripts/setup-mysql.sh
```

### Krok 3: Uruchom Kafka Streams (Terminal 1)
```shell
docker exec --workdir /home/appuser -it broker-1 bash
./scripts/start-streams.sh 7 60 A
```

Parametry: 7 dni okno anomalii, 60% próg, tryb A (szybki)

### Krok 4: Uruchom Kafka Connect (Terminal 2)
```shell
docker exec --workdir /opt/kafka/bin/ -it broker-1 bash
../../../home/appuser/scripts/start-kafka-connect.sh
```

### Krok 5: Uruchom Producer (Terminal 3)
```shell
docker exec --workdir /home/appuser -it broker-1 bash
./scripts/start-producer.sh /tmp/crimes-in-chicago_result/ 1000
```

Parametry: folder z danymi, 1000 rekordów na sekundę

### Krok 6: Sprawdź wyniki (Terminal 4 - w folderze z projektem)
```shell
(w folderze z projektem)
./scripts/monitor-mysql.sh
```

## Oczekiwane wyniki

### W MySQL powinny pojawić się dane w tabelach (skrypt wyświetla 5 najnowszych):
- `crime_aggregates` - agregacje miesięczne po dzielnicy i typie przestępstwa
- `crime_anomalies` - wykryte anomalie (wysokie % FBI crimes)

### Przykład wyniku agregacji:
| year_month | primary_description | district | total_crimes | fbi_index_count |
|------------|---------------------|----------|--------------|-----------------|
| 2023-01    | HOMICIDE            | 1        | 5            | 4               |
| 2023-01    | BATTERY             | 12       | 15           | 2               |

### Przykład wyniku anomalii:
| district | fbi_percentage | window_start         | window_end           |
|----------|----------------|----------------------|----------------------|
| 1        | 80.0           | 2023-01-15T00:00:00Z | 2023-01-22T00:00:00Z |
| 5        | 95.5           | 2023-01-15T00:00:00Z | 2023-01-22T00:00:00Z |

## Troubleshooting

### Problem: Brak danych w MySQL
1. Sprawdź czy Kafka Streams działa (Terminal 1)
2. Sprawdź czy Producer wysyła dane (Terminal 3)
3. Sprawdź czy Kafka Connect działa (Terminal 2)

### Problem: MySQL nie działa
```shell
cd /opt/kafka/bin/
../../../home/appuser/reset-environment.sh
```

### Sprawdzenie danych w tematach
Sprawdź dane wejściowe
```shell
./kafka-console-consumer.sh --bootstrap-server broker-1:19092 --topic crimes-input --from-beginning --max-messages 5
```
Sprawdź agregacje
```shell
./kafka-console-consumer.sh --bootstrap-server broker-1:19092 --topic crimes-aggregates --from-beginning --max-messages 5
```
Sprawdź anomalie
```shell
./kafka-console-consumer.sh --bootstrap-server broker-1:19092 --topic crimes-anomalies --from-beginning --max-messages 5
```

## Zatrzymanie systemu

W każdym terminalu naciśnij `Ctrl+C`

Zatrzymaj MySQL
```shell
docker stop chicago-mysql
docker rm chicago-mysql
```