package com.lzhpo.spark;

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
        SparkSession session = SparkSession
                .builder()
                .master("local")
                .appName("MySQL")
                .getOrCreate();

        /**
         * 数据库连接信息
         */
        Dataset<Row> df = session
            .read()
            .format("jdbc")
            .option("url", "jdbc:mysql://192.168.200.111:3306/bigdata?useUnicode=true&characterEncoding=utf8&useSSL=false")//url连接信息
            .option("dbtable", "user")//使用的数据表
            .option("user", "root")//数据库用户名
            .option("password", "123456")//数据库密码
            .option("driver", "com.mysql.jdbc.Driver")//自己加上的
            .load();

        /**
         * 展示全部数据
         */
        df.show();//默认仅仅展示前20条数据：only showing top 20 rows
//        +---+----+---+------+
//        | id|name|age|iphone|
//        +---+----+---+------+
//        |  1|  小明| 16|   111|
//        |  2|  张三| 23|   222|
//        |  3|  小花| 21|   333|
//        |  4|  李思| 24|   444|
//        |  5|  张思| 24|   555|
//        |  6|  张思| 24|   555|
//        +---+----+---+------+


        /**
         * 查看表的DataFrame
         *          注意：它会自动打印表结构图
         */
        df.printSchema();
//        root
//            |-- id: integer (nullable = true)
//            |-- name: string (nullable = true)
//            |-- age: integer (nullable = true)
//            |-- iphone: string (nullable = true)

        /**
         * 查询全部数据
         */
        Dataset<Row> AllMessage = df.select(new Column("*"));
        AllMessage.show();
//        +---+----+---+------+
//        | id|name|age|iphone|
//        +---+----+---+------+
//        |  1|  小明| 16|   111|
//        |  2|  张三| 23|   222|
//        |  3|  小花| 21|   333|
//        |  4|  李思| 24|   444|
//        |  5|  张思| 24|   555|
//        |  6|  张思| 24|   555|
//        +---+----+---+------+

        /**
         * 模糊查询：手机号最后面一个数字是5的
         */
        Dataset<Row> like = df.where("iphone like '5%'");
        like.show();
//        +---+----+---+------+
//        | id|name|age|iphone|
//        +---+----+---+------+
//        |  5|  张思| 24|   555|
//        |  6|  张思| 24|   555|
//        +---+----+---+------+

        /**
         * 去重
         *      注意，这里是将name重复的字段去掉，然后再放进tb_write表中
         */
        Dataset<Row> name = df.select(new Column("name"));
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

        /**
         * 统计年龄
         */
        Dataset<Row> countsByAge = df.groupBy("age").count();
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
         * 将countsByAge保存为JSON格式的S3
         */
        countsByAge.write().format("json").save("file:///E:/Code/LearningBigData/spark-03-mysql/src/File/tojson");
    }
}
