package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageListener<K, V> {

    void onMessage(ConsumerRecord<K, V> record);
}
