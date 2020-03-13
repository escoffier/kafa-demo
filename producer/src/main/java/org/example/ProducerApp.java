package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

//import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
//import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Hello world!
 *
 */
public class ProducerApp
{
    private static Logger logger = LoggerFactory.getLogger(ProducerApp.class);

    public static void main( String[] args ) throws Exception
    {
        Properties properties= new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG,  EmployeeSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "emp-tx");
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
//        ProducerRecord<String, String> record = new ProducerRecord<>("string-topic", "first", String.valueOf(System.currentTimeMillis()));
//        kafkaProducer.send(record);
        MessageProducer messageProducer = new MessageProducer(properties);
        messageProducer.init();
        logger.info("##################################");
        messageProducer.multiSend();
//        messageProducer.send();
        logger.info("##################################");
        messageProducer.stop();
        System.out.println( "Hello World!" );
    }
}
