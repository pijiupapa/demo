package com.data.demo.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class TestConsumer {
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
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for(int i=0;i<10;i++){
            topicPartitions.add(new TopicPartition(topic, i));
        }
        consumer.assign(topicPartitions);
        consumer.subscribe(Arrays.asList(topic));

        for (TopicPartition partition:topicPartitions){
            System.out.println(consumer.position(partition));
        }
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        System.out.println(endOffsets.values());
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        System.out.println(beginningOffsets.values());

        consumer.close();

    }
}
