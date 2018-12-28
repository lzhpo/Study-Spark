package com.lzhpo.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《SparkSession》
 *      SparkSessionSpark 2.0提供了对Hive功能的内置支持，
 *      包括使用HiveQL编写查询，访问Hive UDF以及从Hive表读取数据的功能。
 *      要使用这些功能，您无需拥有现有的Hive设置。
 */
public class SparkSessionDemo {
    public static void main(String[] args) {
        /**
         * 起点SparkSession
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        /**
         * 创建DataFrame
         */
        Dataset<Row> df = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");

        //show
        df.show();
        //结果：
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */
    }
}
