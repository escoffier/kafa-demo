package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.model.Employee;

public class EmployeeDeserializer implements Deserializer<Employee> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public EmployeeDeserializer() {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public Employee deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Employee.class);
        } catch (Exception ex) {
            throw new SerializationException("Error serializing JSON message", ex);

        }
    }
}
