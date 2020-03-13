package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.example.model.Employee;

public class EmployeeSerializer implements Serializer<Employee> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public EmployeeSerializer() {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(String topic, Employee data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Error serializing JSON message", ex);
        }

        //return new byte[0];
    }
}
