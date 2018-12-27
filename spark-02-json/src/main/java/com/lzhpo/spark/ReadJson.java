package com.lzhpo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class ReadJson {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("ReadJson")
                .master("local")
                .getOrCreate();

        /**
         * 读取JSON文件目录
         */
        //路径可以是单个文本文件，也可以是存储文本文件的目录。
        Dataset<Row> people = spark.read().json("file:///E:/Code/LearningBigData/spark-02-json/src/File/people.json");

        /**
         * 可以使用printSchema()方法可视化推断的模式
         */
        people.printSchema();
        // root
        //  |-- age: long (nullable = true)
        //  |-- name: string (nullable = true)

        /**
         * 创建视图
         */
        people.createOrReplaceTempView("people");

        /**
         * 查询年龄在13~19的人并打印名字
         */
        Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        namesDF.show();
        // +------+
        // |  name|
        // +------+
        // |Justin|
        // +------+

        /**
         * 可以为由每个字符串存储一个JSON对象的Dataset<String>表示的JSON数据集创建DataFrame。
         */
        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        anotherPeople.show();
        // +---------------+----+
        // |        address|name|
        // +---------------+----+
        // |[Columbus,Ohio]| Yin|
        // +---------------+----+
    }
}
