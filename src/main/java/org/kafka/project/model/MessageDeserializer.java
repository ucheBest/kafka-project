package org.kafka.project.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

@Slf4j
public class MessageDeserializer implements Deserializer<Person> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Person deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                log.info("Null received at deserializing");
                return null;
            }
            return mapper.readValue(new String(data, StandardCharsets.UTF_8), Person.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to MessageDto");
        }
    }
}
