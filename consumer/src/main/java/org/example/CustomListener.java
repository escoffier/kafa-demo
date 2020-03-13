package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.model.Employee;

public class CustomListener implements MessageListener<Integer, Employee> {
    @Override
    public void onMessage(ConsumerRecord<Integer, Employee> record) {

    }
}
