package com.data.demo.spark;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.tools.scalap.Classfile;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ScanHbaseToHive {

    public static final int MIN = 60 * 1000;
    public static final int HOUR = 24 * MIN;
    public static void main(String[] args) throws IOException {

        String tableName = "PERSON.IDNUM_INFO";
        String hiveTable = "test.big_table";
        List<String> fields = Arrays.asList(
                "idhashsha256",
                "id_num_mask",
                "province_code",
                "province_name",
                "city_code",
                "city_name",
                "citylevel",
                "county_code",
                "county_name",
                "birth_year",
                "gender",
                "cn_zodiac",
                "en_zodiac",
                "start_work_date",
                "max_amount_per_month",
                "max_salary",
                "companyname",
                "company_number",
                "area",
                "amount_per_month_latest",
                "salary_latest",
                "month_num_latestcompany",
                "company_num_last24month",
                "plate",
                "truck_tag",
                "car_tag",
                "losecredit_tag",
                "multiborrow_count_0to15d",
                "multiborrow_count_15dto1m",
                "multiborrow_count_1to3m",
                "multiborrow_count_3to6m",
                "multiborrow_companycount_0to15d",
                "multiborrow_companycount_15dto1m",
                "multiborrow_companycount_1to3m",
                "multiborrow_companycount_3to6m",
                "overdue_tag",
                "networkloan_tag",
                "crime_tag",
                "martial_status",
                "flightcount_firstclass",
                "flightcount_businessclass",
                "flightcount_economyclass",
                "flightcount_domestic",
                "flightcount_internaloutside",
                "flightcount_internalinside",
                "flight_time_latest",
                "flightcount_in_7d",
                "flightcount_in_3m",
                "flightcount_in_6m",
                "flightcount_in_12m",
                "flightcount_in_b1m",
                "flightcount_in_b2m",
                "flightcount_in_b3m",
                "flightcount_in_b4m",
                "flightcount_in_b5m",
                "flightcount_in_b6m",
                "flighthours_in_7d",
                "flighthours_in_3m",
                "flighthours_in_6m",
                "flighthours_in_12m",
                "flighthours_in_b1m",
                "flighthours_in_b2m",
                "flighthours_in_b3m",
                "flighthours_in_b4m",
                "flighthours_in_b5m",
                "flighthours_in_b6m",
                "flight_from_city_most",
                "flight_to_city_most",
                "flight_month_in_3m_most",
                "flight_month_in_6m_most",
                "flight_month_in_9m_most",
                "flight_month_in_12m_most",
                "flight_aircompany_most",
                "flight_airline_most",
                "flightcount_takeoff_23to6",
                "flightcount_arrive_23to6",
                "flightcount_takeoff_23to24",
                "flightcount_arrive_23to24",
                "flightcount_0to3m",
                "flightcount_3to6m",
                "flightcount_6to9m",
                "flightcount_9to12m",
                "hotelcount_0to3m",
                "hotelcount_3to6m",
                "hotelcount_6to9m",
                "hotelcount_9to12m",
                "traincount_0to3m",
                "traincount_3to6m",
                "traincount_6to9m",
                "traincount_9to12m",
                "blcat_a1_cnt",
                "blcat_a2_cnt",
                "blcat_a11_cnt",
                "blcat_a3_cnt",
                "blcat_a4_cnt",
                "blcat_a5_cnt",
                "blcat_a6_cnt",
                "blcat_a7_cnt",
                "blcat_a8_cnt",
                "blcat_a9_cnt",
                "blcat_a10_cnt",
                "blcat_b50_cnt",
                "blcat_b51_cnt",
                "blcat_b20_cnt",
                "blcat_minorjustice_cnt",
                "blcat_mediumjustice_cnt",
                "blcat_seriousjustice_cnt",
                "blcat_overdue_cnt",
                "blcat_networkloan_cnt",
                "query_loanorg_totalcnt",
                "query_loan_totalcnt",
                "query_loanorg_cnt_7d",
                "query_loan_cnt_7d",
                "query_loanorg_cnt_15d",
                "query_loan_cnt_15d",
                "query_loanorg_cnt_1m",
                "query_loan_cnt_1m",
                "query_loanorg_cnt_3m",
                "query_loan_cnt_3m",
                "query_loanorg_cnt_6m",
                "query_loan_cnt_6m",
                "query_loanorg_cnt_12m",
                "query_loan_cnt_12m"
        );
        Map<String, DataType> fieldTypes = ImmutableMap.<String, DataType> builder()
                .put("idhashsha256", StringType)
                .put("id_num_mask",StringType)
                .put("province_code",StringType)
                .put("province_name",StringType)
                .put("city_code",StringType)
                .put("city_name",StringType)
                .put("citylevel",StringType)
                .put("county_code",StringType)
                .put("county_name",StringType)
                .put("birth_year",StringType)
                .put("gender",IntegerType)
                .put("cn_zodiac",StringType)
                .put("en_zodiac",StringType)
                .put("start_work_date",StringType)
                .put("max_amount_per_month",DoubleType)
                .put("max_salary",DoubleType)
                .put("companyname",StringType)
                .put("company_number",StringType)
                .put("area",StringType)
                .put("amount_per_month_latest",DoubleType)
                .put("salary_latest",DoubleType)
                .put("month_num_latestcompany",IntegerType)
                .put("company_num_last24month",IntegerType)
                .put("plate",StringType)
                .put("truck_tag",StringType)
                .put("car_tag",StringType)
                .put("losecredit_tag",StringType)
                .put("multiborrow_count_0to15d",IntegerType)
                .put("multiborrow_count_15dto1m",IntegerType)
                .put("multiborrow_count_1to3m",IntegerType)
                .put("multiborrow_count_3to6m",IntegerType)
                .put("multiborrow_companycount_0to15d",IntegerType)
                .put("multiborrow_companycount_15dto1m",IntegerType)
                .put("multiborrow_companycount_1to3m",IntegerType)
                .put("multiborrow_companycount_3to6m",IntegerType)
                .put("overdue_tag",StringType)
                .put("networkloan_tag",StringType)
                .put("crime_tag",StringType)
                .put("martial_status",IntegerType)
                .put("flightcount_firstclass",IntegerType)
                .put("flightcount_businessclass",IntegerType)
                .put("flightcount_economyclass",IntegerType)
                .put("flightcount_domestic",IntegerType)
                .put("flightcount_internaloutside",IntegerType)
                .put("flightcount_internalinside",IntegerType)
                .put("flight_time_latest",StringType)
                .put("flightcount_in_7d",IntegerType)
                .put("flightcount_in_3m",IntegerType)
                .put("flightcount_in_6m",IntegerType)
                .put("flightcount_in_12m",IntegerType)
                .put("flightcount_in_b1m",IntegerType)
                .put("flightcount_in_b2m",IntegerType)
                .put("flightcount_in_b3m",IntegerType)
                .put("flightcount_in_b4m",IntegerType)
                .put("flightcount_in_b5m",IntegerType)
                .put("flightcount_in_b6m",IntegerType)
                .put("flighthours_in_7d",DoubleType)
                .put("flighthours_in_3m",DoubleType)
                .put("flighthours_in_6m",DoubleType)
                .put("flighthours_in_12m",DoubleType)
                .put("flighthours_in_b1m",DoubleType)
                .put("flighthours_in_b2m",DoubleType)
                .put("flighthours_in_b3m",DoubleType)
                .put("flighthours_in_b4m",DoubleType)
                .put("flighthours_in_b5m",DoubleType)
                .put("flighthours_in_b6m",DoubleType)
                .put("flight_from_city_most",StringType)
                .put("flight_to_city_most",StringType)
                .put("flight_month_in_3m_most",IntegerType)
                .put("flight_month_in_6m_most",IntegerType)
                .put("flight_month_in_9m_most",IntegerType)
                .put("flight_month_in_12m_most",IntegerType)
                .put("flight_aircompany_most",StringType)
                .put("flight_airline_most",StringType)
                .put("flightcount_takeoff_23to6",IntegerType)
                .put("flightcount_arrive_23to6",IntegerType)
                .put("flightcount_takeoff_23to24",IntegerType)
                .put("flightcount_arrive_23to24",IntegerType)
                .put("flightcount_0to3m",IntegerType)
                .put("flightcount_3to6m",IntegerType)
                .put("flightcount_6to9m",IntegerType)
                .put("flightcount_9to12m",IntegerType)
                .put("hotelcount_0to3m",IntegerType)
                .put("hotelcount_3to6m",IntegerType)
                .put("hotelcount_6to9m",IntegerType)
                .put("hotelcount_9to12m",IntegerType)
                .put("traincount_0to3m",IntegerType)
                .put("traincount_3to6m",IntegerType)
                .put("traincount_6to9m",IntegerType)
                .put("traincount_9to12m",IntegerType)
                .put("blcat_a1_cnt",IntegerType)
                .put("blcat_a2_cnt",IntegerType)
                .put("blcat_a11_cnt",IntegerType)
                .put("blcat_a3_cnt",IntegerType)
                .put("blcat_a4_cnt",IntegerType)
                .put("blcat_a5_cnt",IntegerType)
                .put("blcat_a6_cnt",IntegerType)
                .put("blcat_a7_cnt",IntegerType)
                .put("blcat_a8_cnt",IntegerType)
                .put("blcat_a9_cnt",IntegerType)
                .put("blcat_a10_cnt",IntegerType)
                .put("blcat_b50_cnt",IntegerType)
                .put("blcat_b51_cnt",IntegerType)
                .put("blcat_b20_cnt",IntegerType)
                .put("blcat_minorjustice_cnt",IntegerType)
                .put("blcat_mediumjustice_cnt",IntegerType)
                .put("blcat_seriousjustice_cnt",IntegerType)
                .put("blcat_overdue_cnt",IntegerType)
                .put("blcat_networkloan_cnt",IntegerType)
                .put("query_loanorg_totalcnt",IntegerType)
                .put("query_loan_totalcnt",IntegerType)
                .put("query_loanorg_cnt_7d",IntegerType)
                .put("query_loan_cnt_7d",IntegerType)
                .put("query_loanorg_cnt_15d",IntegerType)
                .put("query_loan_cnt_15d",IntegerType)
                .put("query_loanorg_cnt_1m",IntegerType)
                .put("query_loan_cnt_1m",IntegerType)
                .put("query_loanorg_cnt_3m",IntegerType)
                .put("query_loan_cnt_3m",IntegerType)
                .put("query_loanorg_cnt_6m",IntegerType)
                .put("query_loan_cnt_6m",IntegerType)
                .put("query_loanorg_cnt_12m",IntegerType)
                .put("query_loan_cnt_12m",IntegerType)
                .build();


        SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseDistributedScan " + tableName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        try {
            Configuration conf = HBaseConfiguration.create();
            // protect scan cost too much time, in case of time out
            conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HOUR);
            conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, HOUR);
            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
            HiveContext hiveContext = new HiveContext(jsc);

            Scan scan = new Scan();
            scan.setCaching(1000);
            scan.setCacheBlocks(false);
            for(String field : fields)
                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(field));

//            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("truck_tag"),
//                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("Y"));
//            filter.setFilterIfMissing(true);
//            scan.setFilter(filter);



            JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd =
                    hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan);
            JavaRDD<Row> rowRDD = javaRdd.map(tuple2 -> {
                List<Object> values = new ArrayList<>();
                String idnum = Bytes.toString(tuple2._1.get());
                values.add(idnum);
                for( String field : fields) {
                    if (field.equals("idhashsha256")) {
                        continue;
                    }
                    byte[] raw = tuple2._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes(field));
                    Object value = null;
                    if (raw != null) {
                        value = convertToObject(raw, fieldTypes.get(field));
                    }
                    values.add(value);
               }
                return RowFactory.create(values.toArray());
            });
            StructType structType = createStructType(fields, fieldTypes);
            DataFrame dataFrame = hiveContext.createDataFrame(rowRDD, structType);
            dataFrame.write().mode(SaveMode.Overwrite).saveAsTable(hiveTable);
//            dataFrame.saveAsTable(hiveTable, SaveMode.Overwrite);
        } finally {
            jsc.stop();
        }
    }

    public static StructType createStructType(List<String> fields, Map<String, DataType> fieldTypes) {
        List<StructField> structFields = new ArrayList<>();
        for(String field : fields) {
            StructField structField = DataTypes.createStructField(field, fieldTypes.get(field), true);
            structFields.add(structField);
        }
        return DataTypes.createStructType(structFields);
    }

    public static Object convertToObject(byte[] array, DataType dataType) {
        if (dataType.equals(StringType)) {
            return Bytes.toString(array);
        }
        else if (dataType.equals(DoubleType)) {
            return new Double(Bytes.toDouble(array));
        }
        else if (dataType.equals(IntegerType)) {
            return new Integer(Bytes.toInt(array));
        }
        else if (dataType.equals(LongType)) {
            return new Long(Bytes.toLong(array));
        }
        throw new IllegalArgumentException("unkown data type " + dataType.getClass());

    }

}
