package com.chicago.crimes.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class CrimeTimestampExtractor implements TimestampExtractor {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            // Parsuj JSON z wartości rekordu
            String jsonValue = (String) record.value();
            JsonNode jsonNode = objectMapper.readTree(jsonValue);

            // Wyciągnij pole "Date"
            String dateStr = jsonNode.get("Date").asText();

            // Sparsuj datę do LocalDateTime
            LocalDateTime dateTime = parseDateTime(dateStr);

            // Konwertuj do epoch milliseconds (UTC)
            long timestamp = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            // System.out.println("Extracted timestamp: " + dateStr + " -> " + timestamp + " (" + dateTime + ")");
            return timestamp;

        } catch (Exception e) {
            System.err.println("Błąd ekstraktowania timestamp z rekordu: " + e.getMessage());
            System.err.println("Używam partitionTime: " + partitionTime);
            return partitionTime; // Fallback na timestamp wiadomości Kafka
        }
    }

    private LocalDateTime parseDateTime(String dateStr) {
        try {
            return LocalDateTime.parse(dateStr, ISO_FORMATTER);
        } catch (Exception e) {
                System.err.println("Nie można sparsować daty: " + dateStr);
                return LocalDateTime.now();
        }
    }
}
