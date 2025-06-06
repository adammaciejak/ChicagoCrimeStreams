package com.chicago.crimes.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
    public static final ObjectMapper objectMapper = new ObjectMapper();

//    static {
//        objectMapper.registerModule(new JavaTimeModule());
//    }

    private final Class<T> clazz;

    public JsonSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<>(clazz);
    }

    public static class JsonSerializer<T> implements Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) return null;
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing JSON", e);
            }
        }
    }

    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> clazz;

        public JsonDeserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return objectMapper.readValue(data, clazz);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing JSON", e);
            }
        }
    }
}
