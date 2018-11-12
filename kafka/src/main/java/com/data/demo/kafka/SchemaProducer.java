package com.data.demo.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.util.Properties;

public class SchemaProducer {

    public static void main(String[] args) throws Exception {
        String kafkaHost = "10.10.30.185:9092";
        String topic = "schema-tutorial";
        String schameFilename = "kafka/src/main/resources/user.json";
        String registryHost = "http://10.10.30.185:8081";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", registryHost);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String key = "11234567788899";
        Schema schema = new Schema.Parser().parse(new File(schameFilename));
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "zhmin");
        avroRecord.put("age", 18);
        avroRecord.put("date", "1991-1-1");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, avroRecord);
        RecordMetadata meta = producer.send(record).get();
        System.out.println(meta);
        producer.flush();
        producer.close();

    }
}
