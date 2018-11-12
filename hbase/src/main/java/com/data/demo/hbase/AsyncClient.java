package com.data.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class AsyncClient {
    private AsyncConnection connection;
    private AsyncTable<AdvancedScanResultConsumer> table;

    private AsyncClient(String host, String tableName) throws ExecutionException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        connection = ConnectionFactory.createAsyncConnection(conf).get();
        table = connection.getTable(TableName.valueOf(tableName));
    }

    public String get(String rowKey) throws ExecutionException, InterruptedException {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes("cf"));
        Result result = (Result) table.get(get).get();
        return Bytes.toString(result.value());
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException{
        AsyncClient client = new AsyncClient("10.10.30.185", "tutorial");
        String value = client.get("1234567890");
        System.out.println(value);
    }
}
