package com.data.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        String host = "10.10.30.185:9092";
        String topic = "topic-tutorial";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String key = "mykey";
        String value = "this is test message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata meta = future.get();
        System.out.println(meta);
        producer.flush();
        producer.close();
    }
}
