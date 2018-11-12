package com.data.demo.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.time.Duration;
import java.util.*;

public class SchemaConsumer {

    public static void main(String[] args) {
        String kafkaHost = "10.10.30.185:9092";
        String topic = "schema-tutorial";
        String registryHost = "http://10.10.30.185:8081";
        String groupId = "schema-tutorial";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", registryHost);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, GenericRecord> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            System.out.println(record.value().get("name"));
            System.out.println(record.value().get("age"));
        }


        consumer.close();

    }
}
