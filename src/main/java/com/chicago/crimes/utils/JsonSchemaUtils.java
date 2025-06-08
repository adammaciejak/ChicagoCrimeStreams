package com.chicago.crimes.utils;

import com.chicago.crimes.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSchemaUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static String createAggregateMessage(CrimeAggregate aggregate, String key) {
        ObjectNode message = JsonNodeFactory.instance.objectNode();

        // Schema dla agregatów
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("type", "struct");
        schema.put("optional", false);
        schema.put("version", 1);

        ArrayNode fields = schema.putArray("fields");
        fields.add(createField("year_month", "string"));
        fields.add(createField("primary_description", "string"));
        fields.add(createField("district", "string"));
        fields.add(createField("total_crimes", "int32"));
        fields.add(createField("arrest_count", "int32"));
        fields.add(createField("domestic_count", "int32"));
        fields.add(createField("fbi_index_count", "int32"));

        // Payload dla agregatów
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("year_month", aggregate.getYearMonth());
        payload.put("primary_description", aggregate.getPrimaryDescription());
        payload.put("district", aggregate.getDistrict());
        payload.put("total_crimes", aggregate.getTotalCrimes());
        payload.put("arrest_count", aggregate.getArrestCount());
        payload.put("domestic_count", aggregate.getDomesticCount());
        payload.put("fbi_index_count", aggregate.getFbiIndexCount());

        message.set("schema", schema);
        message.set("payload", payload);

        return message.toString();
    }

    public static String createAnomalyMessage(AnomalyAlert alert, String key) {
        ObjectNode message = JsonNodeFactory.instance.objectNode();

        // Schema dla anomalii
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("type", "struct");
        schema.put("optional", false);
        schema.put("version", 1);

        ArrayNode fields = schema.putArray("fields");
        fields.add(createField("window_start", "string"));
        fields.add(createField("window_end", "string"));
        fields.add(createField("district", "string"));
        fields.add(createField("fbi_index_crimes", "int64"));
        fields.add(createField("total_crimes", "int64"));
        fields.add(createField("fbi_percentage", "float"));

        // Payload dla anomalii
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("window_start", alert.getWindowStart());
        payload.put("window_end", alert.getWindowEnd());
        payload.put("district", alert.getDistrict());
        payload.put("fbi_index_crimes", alert.getFbiIndexCrimes());
        payload.put("total_crimes", alert.getTotalCrimes());
        payload.put("fbi_percentage", alert.getFbiPercentage());

        message.set("schema", schema);
        message.set("payload", payload);

        return message.toString();
    }

    private static ObjectNode createField(String fieldName, String type) {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("field", fieldName);
        field.put("type", type);
        field.put("optional", false);
        return field;
    }
}
