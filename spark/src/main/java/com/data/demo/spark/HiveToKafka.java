package com.data.demo.spark;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.log4j.LogManager;

import java.util.ArrayList;
import java.util.Properties;

public class HiveToKafka {

    public static String schemaContent = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"user\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"age\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";


    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("map partitions demo");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
        String fetchSql = String.format("SELECT * FROM test.demo");
        DataFrame frame = sqlContext.sql(fetchSql);
        JavaRDD<Row> rows  = frame.javaRDD();
        rows.mapPartitions(iterator -> {
            Schema schema = new Schema.Parser().parse(schemaContent);
            KafkaProducer<String, GenericRecord> producer = makeProducer();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                ProducerRecord<String, GenericRecord> record =convertRecord(schema, row);
                producer.send(record);
            }
            producer.flush();
            producer.close();
            return new ArrayList<>();
        }).count();
    }

    public static ProducerRecord<String, GenericRecord> convertRecord(Schema schema, Row row) {
        String topic = "hive-kafka-tutorial";
        String key = row.getString(row.fieldIndex("idnum"));
        Integer age = row.getInt(row.fieldIndex("age"));
        String name = row.getString(row.fieldIndex("name"));
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", name);
        avroRecord.put("age", age);
        LogManager.getRootLogger().warn(key);
        return new ProducerRecord<>(topic, key, avroRecord);
    }

    public static KafkaProducer<String, GenericRecord> makeProducer() {
        String kafkaHost = "10.10.30.185:9092";
        String registryHost = "http://10.10.30.185:8081";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", registryHost);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        return producer;
    }
}
