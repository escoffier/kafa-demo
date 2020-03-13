package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.example.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;

public class ListenerConsumer<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ListenerConsumer.class);

    private MessageListener<K, V> listener;

    KafkaConsumer<K, V> kafkaConsumer;

    private Map<TopicPartition, OffsetAndMetadata> currentOffset;

    public ListenerConsumer(MessageListener<K, V> listener, Properties properties, String topic) {
        this.listener = listener;

        currentOffset = new HashMap<>();
        this.kafkaConsumer = new KafkaConsumer<K, V>(properties);
        if (!topic.isEmpty()) {
            kafkaConsumer.subscribe(Collections.singleton(topic), new HandleRebalance<K, V>(kafkaConsumer));
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<K, V> record : records) {
                    listener.onMessage(record);
                    logger.info("topic = {}, partition={}, offset = {}, key= {}, value = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset(), "no metadata"));
                }
                try {
                    kafkaConsumer.commitSync();
                } catch (Exception ex) {
                    logger.warn(ex.getMessage());
                    ex.printStackTrace();
                    Thread.currentThread().getStackTrace();
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    class HandleRebalance<K, V> implements ConsumerRebalanceListener {

        private final Logger logger = LoggerFactory.getLogger(HandleRebalance.class);

        private KafkaConsumer<K, V> consumer;

        public HandleRebalance(KafkaConsumer<K, V> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("onPartitionsAssigned: " + partitions.toString());

            for (TopicPartition topicPartition : partitions) {
                OffsetAndMetadata offset = ListenerConsumer.this.currentOffset.get(topicPartition);
                if (offset != null) {
                    consumer.seek(topicPartition, ListenerConsumer.this.currentOffset.get(topicPartition));
                }
            }
        }
    }
}
