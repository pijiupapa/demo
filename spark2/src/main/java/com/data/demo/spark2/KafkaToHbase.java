package com.data.demo.spark2;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class KafkaToHbase implements Serializable {

    public Table makeHbaseTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.10.30.185:2181");
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn.getTable(TableName.valueOf("testYYZ"));
    }

    public byte[] toBytes(Object object) throws IllegalArgumentException {
        if (object instanceof CharSequence) {
            return Bytes.toBytes(object.toString());
        }
        if (object instanceof Integer) {
            return Bytes.toBytes((Integer) object);
        }
        if (object instanceof Long) {
            return Bytes.toBytes((Long) object);
        }
        if (object instanceof Double) {
            return Bytes.toBytes((Double) object);
        }
        if (object instanceof Float) {
            return Bytes.toBytes((Float) object);
        }
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        if (object instanceof Boolean) {
            return Bytes.toBytes((Boolean) object);
        }
        throw  new IllegalArgumentException("cannot convert" + object.getClass().toString() + "to byte array");

    }

    public void run() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.10.30.185:9092,10.10.30.186:9092,10.10.30.187:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
        kafkaParams.put("schema.registry.url", "http://10.10.30.185:8081");
        kafkaParams.put("group.id", "test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("auto.commit.interval.ms", 1000);

        Collection<String> topics = Arrays.asList("sparkHiveTest");
        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.mapPartitions(partition->{
            Table table = makeHbaseTable();
            List<Put> putList = new ArrayList<>();
            while (partition.hasNext()){
                ConsumerRecord<String, GenericRecord> record = partition.next();
                String rowKey = record.key();
                Long ts = record.timestamp();
                Put put = new Put(Bytes.toBytes(rowKey));
                List<Schema.Field> fields = record.value().getSchema().getFields();
                fields.stream().map(field -> field.name()).forEach(name -> {
                    Object value = record.value().get(name);
                    if(value!=null) {
                        put.addColumn(Bytes.toBytes("cf"),
                                Bytes.toBytes(name),
                                record.timestamp(),
                                toBytes(value));
                    }
                });
                putList.add(put);
            }
            table.put(putList);
            table.close();
            return new ArrayList<>().iterator();
        }).print();

        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaToHbase spark = new KafkaToHbase();
        spark.run();
    }
}
