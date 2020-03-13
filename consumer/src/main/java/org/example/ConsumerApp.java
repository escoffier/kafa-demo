package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * Hello world!
 */
public class ConsumerApp {
    private static Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        //properties.put(VALUE_DESERIALIZER_CLASS_CONFIG,  KafkaAvroSerializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, EmployeeDeserializer.class);
        properties.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(GROUP_ID_CONFIG, "emp_group_3");

//        KafkaConsumer<Integer, Employee> kafkaConsumer = new KafkaConsumer<Integer, Employee>(properties);
//        kafkaConsumer.subscribe(Collections.singletonList("string-topic"));

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
//            KafkaConsumer<Integer, Employee> consumer = new KafkaConsumer<Integer, Employee>(properties);
//            consumer.subscribe(Collections.singletonList("string-topic"), new HandleRebalance<Integer, Employee>(consumer));
//            MessageProcessor messageProcessor = new MessageProcessor(consumer);
//            executorService.execute(messageProcessor);
            MessageListener<Integer, Employee> messageListener = new CustomListener();
            ListenerConsumer<Integer, Employee> listenerConsumer =
                    new ListenerConsumer<>(messageListener, properties, "string-topic");
            executorService.execute(listenerConsumer);
        }
    }

//    static class MessageProcessor implements Runnable {
//
//        private KafkaConsumer kafkaConsumer;
//        private Map<TopicPartition, OffsetAndMetadata> currentOffset;
//
//        public MessageProcessor(KafkaConsumer kafkaConsumer) {
//
//            this.kafkaConsumer = kafkaConsumer;
//            this.currentOffset = new HashMap<>();
//        }
//
//        @Override
//        public void run() {
//            try {
//                while (true) {
//                    ConsumerRecords<Integer, Employee> records = kafkaConsumer.poll(Duration.ofSeconds(1));
//                    for (ConsumerRecord<Integer, Employee> record : records) {
//                        logger.info("topic = {}, partition={}, offset = {}, key= {}, value = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
//                        currentOffset.put(new TopicPartition(record.topic(), record.partition()),
//                                new OffsetAndMetadata(record.offset(), "no metadata"));
//                    }
//                    try {
//                        kafkaConsumer.commitSync();
//                    } catch (Exception ex) {
//                        logger.warn(ex.getMessage());
//                    }
//                }
//            } finally {
//                kafkaConsumer.close();
//            }
//        }
//    }


}
