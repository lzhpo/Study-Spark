package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class MySQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MySQL")
                .getOrCreate();

        //注意：可以通过load / save或jdbc方法实现JDBC加载和保存
        //从JDBC源
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false")
                .option("dbtable", "user")
                .option("user", "root")
                .option("driver", "com.mysql.jdbc.Driver")//自己加上的
                .option("password", "123456")
                .load();
//        jdbcDF.show();

        /**
         *  *：查询全部
         */
        Dataset<Row> df1 = jdbcDF.select(new Column("*"));
        df1.show();
        /*
        +---+----+---+------+
        | id|name|age|iphone|
        +---+----+---+------+
        |  1|  小明| 16|   111|
        |  2|  张三| 23|   222|
        |  3|  小花| 21|   333|
        |  4|  李思| 24|   444|
        |  5|  张思| 24|   555|
        |  6|  张思| 24|   555|
        +---+----+---+------+
         */

        /**
         * 统计年龄
         */
        Dataset<Row> countsByAge = jdbcDF.groupBy("age").count();
        countsByAge.show();
        //        +---+-----+
        //        |age|count|
        //        +---+-----+
        //        | 16|    1|
        //        | 23|    1|
        //        | 24|    3|
        //        | 21|    1|
        //        +---+-----+


        /**
         * 去重
         *      注意，这里是将name重复的字段去掉，然后再放进tb_write表中
         */
        Dataset<Row> name = jdbcDF.select(new Column("name"));
        Dataset<Row> distinct = name.distinct();
        distinct.show();
        //去重之后的结果
        //        +----+
        //        |name|
        //        +----+
        //        |  小明|
        //        |  小花|
        //        |  李思|
        //        |  张三|
        //        |  张思|
        //        +----+

        /**
         * 将去重之后的结果写入其它表
         *
         *      注意：这里是将重复name的字段写入tb_write表中，如果没有tb_write表，会自动创建
         */
        Properties prop = new Properties();
        prop.put("user","root");
        prop.put("password","123456");
        prop.put("driver","com.mysql.jdbc.Driver");
        distinct.write().jdbc("jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false","tb_write",prop);
        distinct.show();
        //去重之后的结果
        //        +----+
        //        |name|
        //        +----+
        //        |  小明|
        //        |  小花|
        //        |  李思|
        //        |  张三|
        //        |  张思|
        //        +----+



        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false", "tb_write", connectionProperties);

        //将数据保存到JDBC源
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false")
                .option("dbtable", "tb_write1")
                .option("user", "root")
                .option("password", "123456")
                .save();
        //写入
        jdbcDF2.write()
                .jdbc("jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false", "tb_write2", connectionProperties);

    }
}
