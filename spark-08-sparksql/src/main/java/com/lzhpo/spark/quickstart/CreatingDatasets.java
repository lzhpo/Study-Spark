package com.lzhpo.spark.quickstart;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《创建数据集》
 *      数据集与RDD类似，但是，它们不使用Java序列化或Kryo，而是使用专门的编码器来序列化对象以便通过网络进行处理或传输。
 *      虽然编码器和标准序列化都负责将对象转换为字节，但编码器是动态生成的代码，并使用一种格式，
 *      允许Spark执行许多操作，如过滤，排序和散列，而无需将字节反序列化为对象。
 */
public class CreatingDatasets {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreatingDatasets")
                .getOrCreate();

        //加载json数据
        Dataset<Row> df = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");
        df.show();

        //创建一个bean类的实例
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        //验证
        df.show();

        //为Java bean创建
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+

        // Encoders for most common types are provided in class Encoders
        //类Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]
        //查看
        System.out.println("transformedDS.collect()--->"+transformedDS.collect());

        //可以通过提供类将DataFrame转换为数据集。基于名称的映射
        String path = "file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

    }
}
