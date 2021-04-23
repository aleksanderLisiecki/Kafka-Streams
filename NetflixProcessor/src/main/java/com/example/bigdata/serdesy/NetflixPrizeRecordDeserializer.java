package com.example.bigdata.serdesy;

import com.example.bigdata.ETLAggregation;
import com.example.bigdata.NetflixPrizeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Map;

public class NetflixPrizeRecordDeserializer implements Deserializer<NetflixPrizeRecord> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public NetflixPrizeRecord deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        final ETLAggregation result = new ETLAggregation();

        final DataInputStream
                dataInputStream =
                new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            return objectMapper.readValue(new String(bytes,"UTF-8"), NetflixPrizeRecord.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Netflix value", e);
        }
    }

    @Override
    public void close() {

    }
}
