package com.lzhpo.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《DataFrameDemo》
 *      无类型数据集操作（又名DataFrame操作）
 *      DataFrames为Scala，Java，Python和R中的结构化数据操作提供特定于域的语言。
 */
public class DataFrameDemo {
    public static void main(String[] args) {
        /**
         * 入口
         */
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("DataFrameDemo")
                .getOrCreate();


        Dataset<Row> df = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");


        //打印结构
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        //查询name
        df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+


        //需要导入import static org.apache.spark.sql.functions.col;
        df.select(col("name"),col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        //查询年龄大于21的
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+



        //统计
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+

    }
}
