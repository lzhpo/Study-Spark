package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《JSON数据集》
 *      Spark SQL可以自动推断JSON数据集的模式并将其加载为Dataset<Row>。
 *      可以使用SparkSession.read().json()a Dataset<String>或JSON文件完成此转换。
 *
 *      注意：作为json文件提供的文件不是典型的JSON文件。每行必须包含一个单独的，自包含的有效JSON对象。
 */
public class JSON {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JSON")
                .master("local")
                .getOrCreate();

        //路径指向JSON数据集。
        //路径可以是单个文本文件，也可以是存储文本文件
        Dataset<Row> people = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");

        //可以使用printSchema（）方法
        people.printSchema();
        // root
        //  |-- age: long (nullable = true)
        //  |-- name: string (nullable = true)

        //创建视图
        people.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        namesDF.show();
        // +------+
        // |  name|
        // +------+
        // |Justin|
        // +------+

        //或者，可以为
        //数据集<String> 表示的JSON数据集创建DataFrame，每个字符串存储一个JSON对象。
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
