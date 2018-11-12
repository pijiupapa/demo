package com.data.demo.hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {

    public static void main(String[] args) throws Exception {
        String host = "10.10.30.185";
        int port = 10000;
        String database = "default";
        String table = "user";

        String jdbcUrl = String.format("jdbc:hive2://%s:%s/%s", host, port, database);
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement();
        String sql = String.format("SELECT * FROM %s LIMIT 5", table);
        ResultSet resultSet = stmt.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();

//        int cols = metaData.getColumnCount();
//
//        for (int i = 1; i <= cols; i++) {
//            System.out.println(metaData.getColumnName(i));
//            System.out.println(metaData.getColumnTypeName(i));
//        }
//
//        int fileds = metaData.getColumnCount();
//        while (resultSet.next()) {
//            StringBuilder builder = new StringBuilder();
//            for (int i = 1; i < fileds+1; i++) {
//                // jdbc的索引是从1开始
//                String value = resultSet.getString(i);
//                builder.append(value + "\t");
//            }
//            System.out.println(builder);
//        }
//        resultSet.close();
//        connection.close();

        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet1 = databaseMetaData.getColumns(null, "%", table, "%");
        List<Map> fields = new ArrayList<>();
        while (resultSet1.next()){
            Map configMap = new HashMap<>();
            String col_name = resultSet1.getString("COLUMN_NAME");
            String data_type = resultSet1.getString("TYPE_NAME");
            System.out.println(col_name);
            System.out.println(data_type);
            configMap.put("name",col_name);
            configMap.put("type",data_type);
            fields.add(configMap);
        }
        System.out.println(fields);


    }
}
