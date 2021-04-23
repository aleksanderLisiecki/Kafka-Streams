package com.example.bigdata.serdesy;

import com.example.bigdata.NetflixPrizeRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class NetflixPrizeRecordSerializer implements Serializer<NetflixPrizeRecord> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, NetflixPrizeRecord obj) {
        if (obj == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(obj);

        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing value", e);
        }
    }
    
}
