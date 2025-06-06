package com.chicago.crimes;

import com.chicago.crimes.model.*;
import com.chicago.crimes.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ChicagoCrimesStreamsApp {

    private static final String APPLICATION_ID = "chicago-crimes-app";
    private static final String INPUT_TOPIC = "crimes-input";
    private static final String AGGREGATES_TOPIC = "crimes-aggregates";
    private static final String ANOMALIES_TOPIC = "crimes-anomalies";

    private static Map<String, IucrCode> iucrCodes = new HashMap<>();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java ChicagoCrimesStreamsApp <bootstrap-servers> <anomaly-days> <anomaly-percentage> [delay-mode]");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        int anomalyDays = Integer.parseInt(args[1]);
        double anomalyPercentage = Double.parseDouble(args[2]);
        String delayMode = args.length > 3 ? args[3] : "A"; // A lub C

        // Załaduj kody IUCR (w rzeczywistej aplikacji z pliku)
        loadIucrCodes();

        Properties props = createProperties(bootstrapServers, delayMode);
        StreamsBuilder builder = new StreamsBuilder();

        buildTopology(builder, anomalyDays, anomalyPercentage);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Dodaj shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Chicago Crimes Streams App...");
            streams.close(Duration.ofSeconds(10));
        }));

        System.out.println("Starting Chicago Crimes Streams App...");
        System.out.println("Topology: " + builder.build().describe());

        streams.start();
    }

    private static Properties createProperties(String bootstrapServers, String delayMode) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Konfiguracja opóźnień w zależności od trybu
        if ("A".equals(delayMode)) {
            // Tryb A - minimalne opóźnienie, wyniki mogą być niepełne
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        } else {
            // Tryb C - wyniki ostateczne, większe opóźnienie
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);
        }

        return props;
    }

    private static void buildTopology(StreamsBuilder builder, int anomalyDays, double anomalyPercentage) {
        // 1. Strumień danych wejściowych
        KStream<String, String> crimeEvents = builder.stream(INPUT_TOPIC);

        // 2. Parsowanie danych JSON
        KStream<String, CrimeRecord> parsedCrimes = crimeEvents
                .mapValues(json -> parseJson(json, CrimeRecord.class))
                .filter((key, crime) -> crime != null && crime.getDistrict() != null);

        // 3. ETL - Agregacje miesięczne
        buildMonthlyAggregates(parsedCrimes);

        // 4. Wykrywanie anomalii
        buildAnomalyDetection(parsedCrimes, anomalyDays, anomalyPercentage);
    }

    private static void buildMonthlyAggregates(KStream<String, CrimeRecord> crimes) {
        crimes
                .map((key, crime) -> {
                    String aggregateKey = String.format("%s_%s_%s",
                            crime.getYearMonth(),
                            getIucrPrimaryDescription(crime.getIucr()),
                            crime.getDistrict());
                    return KeyValue.pair(aggregateKey, crime);
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(CrimeRecord.class)))
                .aggregate(
                        () -> new CrimeAggregate(),
                        (key, crime, aggregate) -> {
                            String[] keyParts = key.split("_", 3);
                            if (aggregate.getYearMonth() == null) {
                                aggregate.setYearMonth(keyParts[0]);
                                aggregate.setPrimaryDescription(keyParts[1]);
                                aggregate.setDistrict(keyParts[2]);
                            }
                            boolean isFbiIndex = isIucrFbiIndex(crime.getIucr());
                            return aggregate.update(crime, isFbiIndex);
                        },
                        Materialized.with(Serdes.String(), new JsonSerde<>(CrimeAggregate.class))
                )
                .toStream()
                .to(AGGREGATES_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(CrimeAggregate.class)));
    }

    private static void buildAnomalyDetection(KStream<String, CrimeRecord> crimes, int days, double threshold) {
        crimes
                .selectKey((key, crime) -> crime.getDistrict())
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(CrimeRecord.class)))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(days)))
                .aggregate(
                        () -> new DistrictCrimeCounts(),
                        (district, crime, counts) -> {
                            counts.incrementTotal();
                            if (isIucrFbiIndex(crime.getIucr())) {
                                counts.incrementFbiIndex();
                            }
                            return counts;
                        },
                        Materialized.with(Serdes.String(), new JsonSerde<>(DistrictCrimeCounts.class))
                )
                .toStream()
                .filter((windowedDistrict, counts) -> {
                    double percentage = counts.getTotalCrimes() > 0 ?
                            (double) counts.getFbiIndexCrimes() / counts.getTotalCrimes() * 100 : 0;
                    return percentage > threshold;
                })
                .map((windowedDistrict, counts) -> {
                    String district = windowedDistrict.key();
                    String windowStart = windowedDistrict.window().startTime().toString();
                    String windowEnd = windowedDistrict.window().endTime().toString();

                    AnomalyAlert alert = new AnomalyAlert(windowStart, windowEnd, district,
                            counts.getFbiIndexCrimes(), counts.getTotalCrimes());

                    return KeyValue.pair(district, alert);
                })
                .to(ANOMALIES_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(AnomalyAlert.class)));
    }

    // Pomocnicze klasy i metody
    private static <T> T parseJson(String json, Class<T> clazz) {
        try {
            return JsonSerde.objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            return null;
        }
    }

    private static void loadIucrCodes() {
        // Przykładowe kody IUCR - w rzeczywistej aplikacji załaduj z pliku CSV
        iucrCodes.put("0110", new IucrCode("0110", "HOMICIDE", "FIRST DEGREE MURDER", "I"));
        iucrCodes.put("0130", new IucrCode("0130", "HOMICIDE", "SECOND DEGREE MURDER", "I"));
        iucrCodes.put("0261", new IucrCode("0261", "CRIM SEXUAL ASSAULT", "AGG CRIMINAL SEXUAL ASSAULT", "I"));
        iucrCodes.put("0486", new IucrCode("0486", "BATTERY", "DOMESTIC BATTERY SIMPLE", "N"));
        // Dodaj więcej kodów...
    }

    private static String getIucrPrimaryDescription(String iucr) {
        IucrCode code = iucrCodes.get(iucr);
        return code != null ? code.getPrimaryDescription() : "UNKNOWN";
    }

    private static boolean isIucrFbiIndex(String iucr) {
        IucrCode code = iucrCodes.get(iucr);
        return code != null && "I".equals(code.getIndexCode());
    }

    // Klasa pomocnicza dla agregacji anomalii
    public static class DistrictCrimeCounts {
        private long totalCrimes = 0;
        private long fbiIndexCrimes = 0;

        public void incrementTotal() { totalCrimes++; }
        public void incrementFbiIndex() { fbiIndexCrimes++; }

        public long getTotalCrimes() { return totalCrimes; }
        public long getFbiIndexCrimes() { return fbiIndexCrimes; }
    }

    // Klasa dla kodów IUCR
    public static class IucrCode {
        private String iucr;
        private String primaryDescription;
        private String secondaryDescription;
        private String indexCode;

        public IucrCode(String iucr, String primaryDescription, String secondaryDescription, String indexCode) {
            this.iucr = iucr;
            this.primaryDescription = primaryDescription;
            this.secondaryDescription = secondaryDescription;
            this.indexCode = indexCode;
        }

        // Gettery
        public String getIucr() { return iucr; }
        public String getPrimaryDescription() { return primaryDescription; }
        public String getSecondaryDescription() { return secondaryDescription; }
        public String getIndexCode() { return indexCode; }
    }
}
