package com.chicago.crimes.producer;

import com.chicago.crimes.model.CrimeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class CrimesDataProducer {
    private static final String TOPIC = "crimes-input";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java CrimesDataProducer <bootstrap-servers> <data-folder> <records-per-second>");
            System.err.println("Example: CrimesDataProducer broker-1:19092 /tmp/crimes-in-chicago_result/ 10");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String dataFolder = args[1];
        int recordsPerSecond = Integer.parseInt(args[2]);

        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Data folder: " + dataFolder);
        System.out.println("Records per second: " + recordsPerSecond);

        // Konfiguracja producenta z optymalizacjami
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            processDataFolder(producer, dataFolder, recordsPerSecond);
        } finally {
            producer.close();
            System.out.println("Producer closed");
        }
    }

    /**
     * Przetwarza wszystkie pliki part-* w folderze
     */
    private static void processDataFolder(KafkaProducer<String, String> producer,
                                          String folderPath, int recordsPerSecond) throws Exception {

        Path folder = Paths.get(folderPath);
        if (!Files.exists(folder) || !Files.isDirectory(folder)) {
            throw new IllegalArgumentException("Folder does not exist: " + folderPath);
        }

        // Znajdź pliki part-*
        List<Path> partFiles = findPartFiles(folder);

        if (partFiles.isEmpty()) {
            System.err.println("No part-* files found in: " + folderPath);
            return;
        }

        System.out.println("Found " + partFiles.size() + " part-* files");

        long totalRecordsSent = 0;
        long totalRecordsSkipped = 0;
        long startTime = System.currentTimeMillis();

        // Przetwórz każdy plik
        for (int i = 0; i < partFiles.size(); i++) {
            Path partFile = partFiles.get(i);
            System.out.println("Processing file " + (i + 1) + "/" + partFiles.size() +
                    ": " + partFile.getFileName());

            ProcessingResult result = processPartFile(producer, partFile, recordsPerSecond);
            totalRecordsSent += result.sent;
            totalRecordsSkipped += result.skipped;

            System.out.println("  Sent: " + result.sent + ", Skipped: " + result.skipped +
                    " from " + partFile.getFileName());
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("\n=== FINAL STATISTICS ===");
        System.out.println("Total records sent: " + totalRecordsSent);
        System.out.println("Total records skipped: " + totalRecordsSkipped);
        System.out.println("Total processing time: " + (duration / 1000.0) + " seconds");

        if (totalRecordsSent + totalRecordsSkipped > 0) {
            double successRate = (double) totalRecordsSent / (totalRecordsSent + totalRecordsSkipped) * 100;
            System.out.println("Success rate: " + String.format("%.2f", successRate) + "%");
        }
    }

    /**
     * Znajduje pliki part-*
     */
    private static List<Path> findPartFiles(Path folder) throws IOException {
        List<Path> partFiles = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path file : stream) {
                String fileName = file.getFileName().toString();

                // Szukaj plików part-*
                if (fileName.startsWith("part-") &&
                        !fileName.toLowerCase().contains("iucr") &&
                        !fileName.startsWith("_") &&
                        !fileName.startsWith(".")) {

                    partFiles.add(file);
                }
            }
        }

        // Sortuj alfabetycznie
        partFiles.sort(Comparator.comparing(path -> path.getFileName().toString()));
        return partFiles;
    }

    /**
     * Przetwarza pojedynczy plik part-*
     */
    private static ProcessingResult processPartFile(KafkaProducer<String, String> producer,
                                                    Path partFile, int recordsPerSecond) {

        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        long sent = 0;
        long skipped = 0;
        long delayMs = recordsPerSecond > 0 ? 1000 / recordsPerSecond : 0;

        try {
            Iterator<CrimeRecord> iterator = csvMapper
                    .readerFor(CrimeRecord.class)
                    .with(schema)
                    .readValues(partFile.toFile());

            while (iterator.hasNext()) {
                try {
                    // KLUCZOWA ZMIANA: Przechwytuj błędy deserializacji dla każdego rekordu
                    CrimeRecord crime = iterator.next();

                    if (sendCrimeRecord(producer, crime)) {
                        sent++;
                    } else {
                        skipped++;
                    }

                    // Progress co 5000 rekordów
                    if ((sent + skipped) % 5000 == 0) {
                        System.out.println("    Processed " + (sent + skipped) +
                                " records (sent: " + sent + ", skipped: " + skipped + ")");
                    }

                    // Podstawowe opóźnienie
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }

                } catch (Exception e) {
                    // KLUCZOWA ZMIANA: Przechwytuj błędy dla pojedynczego rekordu
                    skipped++;

                    // Loguj błąd ale kontynuuj przetwarzanie
                    if (skipped % 100 == 1) { // Loguj co 100ty błąd żeby nie zapełnić logów
                        System.err.println("Skipping invalid record #" + (sent + skipped) +
                                " in file " + partFile.getFileName() +
                                ": " + e.getMessage());
                    }

                    // Kontynuuj do następnego rekordu
                    continue;
                }
            }

        } catch (Exception e) {
            System.err.println("Error opening file " + partFile.getFileName() + ": " + e.getMessage());
            // Nie wyrzucaj wyjątku - kontynuuj z następnym plikiem
        }

        return new ProcessingResult(sent, skipped);
    }

    /**
     * Wysyła rekord do Kafka z walidacją danych
     */
    private static boolean sendCrimeRecord(KafkaProducer<String, String> producer, CrimeRecord crime) {
        try {
            // KLUCZOWA ZMIANA: Walidacja danych przed wysłaniem
            if (!isValidCrimeRecord(crime)) {
                return false; // Pomiń nieprawidłowy rekord
            }

            String json = JSON_MAPPER.writeValueAsString(crime);
            String key = crime.getDistrict() != null ? crime.getDistrict() : crime.getId();

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, json);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending record " + crime.getId() + ": " + exception.getMessage());
                }
            });

            return true;

        } catch (Exception e) {
            // Loguj błąd ale nie przerywaj przetwarzania
            System.err.println("Error serializing record " +
                    (crime != null ? crime.getId() : "unknown") + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * KLUCZOWA ZMIANA: Walidacja poprawności rekordu
     */
    private static boolean isValidCrimeRecord(CrimeRecord crime) {
        // Sprawdź podstawowe pola
        if (crime == null) {
            return false;
        }

        if (crime.getId() == null || crime.getId().trim().isEmpty()) {
            return false;
        }

        if (crime.getDate() == null || crime.getDate().trim().isEmpty()) {
            return false;
        }

        // Opcjonalnie: sprawdź czy współrzędne są prawidłowe
        // Jeśli są null - to OK (missing data)
        // Jeśli nie są null - muszą być poprawne
        if (crime.getLatitude() != null) {
            if (!isValidLatitude(crime.getLatitude())) {
                return false;
            }
        }

        if (crime.getLongitude() != null) {
            if (!isValidLongitude(crime.getLongitude())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Sprawdza czy latitude jest w prawidłowym zakresie
     */
    private static boolean isValidLatitude(Double latitude) {
        return latitude >= -90.0 && latitude <= 90.0;
    }

    /**
     * Sprawdza czy longitude jest w prawidłowym zakresie
     */
    private static boolean isValidLongitude(Double longitude) {
        return longitude >= -180.0 && longitude <= 180.0;
    }

    /**
     * Klasa pomocnicza do przechowywania wyników przetwarzania
     */
    private static class ProcessingResult {
        final long sent;
        final long skipped;

        ProcessingResult(long sent, long skipped) {
            this.sent = sent;
            this.skipped = skipped;
        }
    }
}
