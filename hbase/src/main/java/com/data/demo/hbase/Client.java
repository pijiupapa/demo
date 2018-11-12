package com.data.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class Client {

    public static void main(String[] args) throws Exception {
        String host = "10.10.30.185:2181";
        String table = "TESTYYZ";
        String rowKey = "x";
        String qulifier = "0";
        Integer value = 10;

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table hbaseTable = connection.getTable(TableName.valueOf(table));

        // put record
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(qulifier), Bytes.toBytes(value));
//        hbaseTable.put(put);

        // get record
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hbaseTable.get(get);
//        byte[] name = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(qulifier));
//        System.out.println(Bytes.toInt(name));
        for (Cell cell:result.listCells()) {
            if(cell!=null) {
                System.out.println(Bytes.toString(cell.getQualifierArray()));
                System.out.println(Bytes.toString(cell.getValueArray()));
            }
        }

        //close
        hbaseTable.close();
        connection.close();
    }
}
