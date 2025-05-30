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
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class CrimesDataProducer {
    private static final String TOPIC = "crimes-input";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java CrimesDataProducer <bootstrap-servers> <csv-file> <records-per-second>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String csvFile = args[1];
        int recordsPerSecond = Integer.parseInt(args[2]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Konfiguracja CSV parsera
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        int count = 0;
        try {
            Iterator<CrimeRecord> iterator = csvMapper
                    .readerFor(CrimeRecord.class)
                    .with(schema)
                    .readValues(new File(csvFile));

            long delayMs = 1000 / recordsPerSecond;

            while (iterator.hasNext()) {
                CrimeRecord crime = iterator.next();

                // Symulacja opóźnień (dane mogą być nieuporządkowane)
                if (ThreadLocalRandom.current().nextDouble() < 0.1) {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(1000, 5000));
                }

                String json = JSON_MAPPER.writeValueAsString(crime);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, crime.getId(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                    }
                });

                count++;
                if (count % 100 == 0) {
                    System.out.println("Sent " + count + " records");
                }

                Thread.sleep(delayMs);
            }

        } finally {
            producer.close();
            System.out.println("Producer finished. Total records sent: " + count);
        }
    }
}
