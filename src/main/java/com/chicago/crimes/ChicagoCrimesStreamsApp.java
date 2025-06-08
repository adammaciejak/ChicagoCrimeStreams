package com.chicago.crimes;

import com.chicago.crimes.model.*;
import com.chicago.crimes.serde.JsonSerde;
import com.chicago.crimes.utils.JsonSchemaUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
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
        if (args.length < 4) { // POPRAWKA: Dodano parametr dla pliku IUCR
            System.err.println("Usage: java ChicagoCrimesStreamsApp <bootstrap-servers> <anomaly-days> <anomaly-percentage> <delay-mode> [iucr-csv-path]");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        int anomalyDays = Integer.parseInt(args[1]);
        double anomalyPercentage = Double.parseDouble(args[2]);
        String delayMode = args[3];
        String iucrCsvPath = args.length > 4 ? args[4] : "Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv";

        // POPRAWKA: Załaduj kody IUCR z rzeczywistego pliku
        loadIucrCodes(iucrCsvPath);

        Properties props = createProperties(bootstrapServers, delayMode);
        StreamsBuilder builder = new StreamsBuilder();

        buildTopology(builder, anomalyDays, anomalyPercentage, delayMode);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

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
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                com.chicago.crimes.extractor.CrimeTimestampExtractor.class);

        return props;
    }

    private static Suppressed<Windowed> getSuppressStrategy(String delayMode) {
        if ("A".equals(delayMode)) {
            return Suppressed.untilTimeLimit(Duration.ZERO, Suppressed.BufferConfig.unbounded());
        } else {
            return Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded());
        }
    }

    private static void buildTopology(StreamsBuilder builder, int anomalyDays, double anomalyPercentage, String delayMode) {
        KStream<String, String> crimeEvents = builder.stream(INPUT_TOPIC);

        KStream<String, CrimeRecord> parsedCrimes = crimeEvents
                .mapValues(json -> parseJson(json, CrimeRecord.class))
                .filter((key, crime) -> crime != null && crime.getDistrict() != null);

        buildMonthlyAggregates(parsedCrimes, delayMode);
        buildAnomalyDetection(parsedCrimes, anomalyDays, anomalyPercentage, delayMode);
    }

    private static void buildMonthlyAggregates(KStream<String, CrimeRecord> crimes, String delayMode) {
        crimes
                .map((key, crime) -> {
                    String aggregateKey = String.format("%s_%s",
                            getIucrPrimaryDescription(crime.getIucr()),
                            crime.getDistrict());
                    return KeyValue.pair(aggregateKey, crime);
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(CrimeRecord.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(30), Duration.ofDays(1)))
                .aggregate(
                        () -> new CrimeAggregate(),
                        (key, crime, aggregate) -> {
                            String[] keyParts = key.split("_", 2);
                            if (aggregate.getYearMonth() == null) {
                                aggregate.setYearMonth(crime.getYearMonth());
                                aggregate.setPrimaryDescription(keyParts[0]);
                                aggregate.setDistrict(keyParts[1]);
                            }
                            boolean isFbiIndex = isIucrFbiIndex(crime.getIucr());
                            return aggregate.update(crime, isFbiIndex);
                        },
                        Materialized.with(Serdes.String(), new JsonSerde<>(CrimeAggregate.class))
                )
                .suppress(getSuppressStrategy(delayMode))
                .toStream()
                .map((windowedKey, aggregate) -> {
                    String outputKey = String.format("%s_%s_%s",
                            aggregate.getYearMonth(),
                            aggregate.getPrimaryDescription(),
                            aggregate.getDistrict());
                    String messageWithSchema = JsonSchemaUtils.createAggregateMessage(aggregate, outputKey);
                    return KeyValue.pair(outputKey, messageWithSchema);
                })
                .to(AGGREGATES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void buildAnomalyDetection(KStream<String, CrimeRecord> crimes, int days, double threshold, String delayMode) {
        crimes
                .selectKey((key, crime) -> crime.getDistrict())
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(CrimeRecord.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(days), Duration.ofDays(1)))
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
                .suppress(getSuppressStrategy(delayMode))
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

                    String messageWithSchema = JsonSchemaUtils.createAnomalyMessage(alert, district);
                    return KeyValue.pair(district, messageWithSchema);
                })
                .to(ANOMALIES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    // POPRAWKA: Rzeczywiste ładowanie z pliku CSV
    private static void loadIucrCodes(String csvFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // Pomiń nagłówek
                }

                String[] values = parseCSVLine(line);
                if (values.length >= 4) {
                    String iucr = values[0].trim();
                    if (iucr.matches("\\d+")) { // Sprawdź czy to są tylko cyfry
                        iucr = String.format("%04d", Integer.parseInt(iucr));
                    }
                    String primaryDescription = values[1].trim();
                    String secondaryDescription = values[2].trim();
                    String indexCode = values[3].trim();

                    iucrCodes.put(iucr, new IucrCode(iucr, primaryDescription, secondaryDescription, indexCode));
                }
            }

            System.out.println("Załadowano " + iucrCodes.size() + " kodów IUCR");

        } catch (IOException e) {
            System.err.println("Błąd ładowania kodów IUCR: " + e.getMessage());
            // loadBasicIucrCodes(); // Fallback
        }
    }

    // POPRAWKA: Dodano fallback dla podstawowych kodów
    private static void loadBasicIucrCodes() {
        iucrCodes.put("110", new IucrCode("110", "HOMICIDE", "FIRST DEGREE MURDER", "I"));
        iucrCodes.put("130", new IucrCode("130", "HOMICIDE", "SECOND DEGREE MURDER", "I"));
        iucrCodes.put("261", new IucrCode("261", "CRIM SEXUAL ASSAULT", "AGG CRIMINAL SEXUAL ASSAULT", "I"));
        iucrCodes.put("486", new IucrCode("486", "BATTERY", "DOMESTIC BATTERY SIMPLE", "N"));
        System.out.println("Załadowano podstawowe kody IUCR (fallback)");
    }

    // Metody pomocnicze
    private static <T> T parseJson(String json, Class<T> clazz) {
        try {
            return JsonSerde.objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            return null;
        }
    }

    private static String[] parseCSVLine(String line) {
        // Prosta implementacja - można ulepszyć obsługę cudzysłowów
        return line.split(",");
    }

    private static String getIucrPrimaryDescription(String iucr) {
        IucrCode code = iucrCodes.get(iucr);
        return code != null ? code.getPrimaryDescription() : "UNKNOWN";
    }

    private static boolean isIucrFbiIndex(String iucr) {
        IucrCode code = iucrCodes.get(iucr);
        return code != null && "I".equals(code.getIndexCode());
    }

    public static class DistrictCrimeCounts {
        private long totalCrimes = 0;
        private long fbiIndexCrimes = 0;

        public void incrementTotal() { totalCrimes++; }
        public void incrementFbiIndex() { fbiIndexCrimes++; }

        public long getTotalCrimes() { return totalCrimes; }
        public long getFbiIndexCrimes() { return fbiIndexCrimes; }
    }

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

        public String getIucr() { return iucr; }
        public String getPrimaryDescription() { return primaryDescription; }
        public String getSecondaryDescription() { return secondaryDescription; }
        public String getIndexCode() { return indexCode; }
    }
}
