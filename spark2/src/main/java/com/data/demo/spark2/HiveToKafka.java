package com.data.demo.spark2;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class HiveToKafka implements Serializable {
    private static String s = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"sparkHiveTest\",\n" +
            "    \"fields\": [\n" +
            "      {\n" +
            "        \"name\": \"name\",\n" +
            "        \"type\": \"string\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"age\",\n" +
            "        \"type\": \"int\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }";

    public static KafkaProducer<String, GenericRecord> makeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.30.185:9092,10.10.30.186:9092,10.10.30.187:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://10.10.30.185:8081");
        return new KafkaProducer<>(props);
    }

    public ProducerRecord<String, GenericRecord> convertRecord(Schema schema, Row row) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        String[] s = {"name", "age"};
        Arrays.stream(s).forEach(field -> {
            int index = row.fieldIndex(field);
            Object value = row.get(index);
            avroRecord.put(field, value);
        });
        String key = row.getString(row.fieldIndex("idnum"));
        Date date = new Date();
        Long ts = date.getTime();
        return new ProducerRecord<>("sparkHiveTest", null, ts, key, avroRecord);
    }

    public void run() {
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .enableHiveSupport()
                .getOrCreate();
        String sql = String.format("SELECT * FROM %s ", "default.user");
        Dataset<Row> ds = spark.sql(sql);
        JavaRDD<Row> rows = ds.javaRDD();
        rows.mapPartitions(partition -> {
            Schema schema = new Schema.Parser().parse(s);
            KafkaProducer<String, GenericRecord> producer = makeProducer();
            while (partition.hasNext()){
                Row row = partition.next();
                ProducerRecord<String, GenericRecord> record = convertRecord(schema, row);
                producer.send(record);
            }
            producer.flush();
            producer.close();
            return new ArrayList<>().iterator();
        }).count();
    }

    public static void main(String[] args) {
        HiveToKafka job = new HiveToKafka();
        job.run();
    }
}
