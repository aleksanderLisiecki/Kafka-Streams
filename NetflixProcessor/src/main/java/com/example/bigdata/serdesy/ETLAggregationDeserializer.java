package com.example.bigdata.serdesy;

import com.example.bigdata.ETLAggregation;
import com.example.bigdata.NetflixPrizeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ETLAggregationDeserializer implements Deserializer<ETLAggregation> {
    private boolean isKey;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public ETLAggregation deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return objectMapper.readValue(new String(bytes,"UTF-8"), ETLAggregation.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing ETL value", e);
        }
    }

    @Override
    public void close() {

    }
}
