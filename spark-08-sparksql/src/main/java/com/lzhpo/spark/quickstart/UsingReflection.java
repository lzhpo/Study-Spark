package com.lzhpo.spark.quickstart;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《使用反射推断模式》
 *
 *      Spark SQL支持自动将JavaBeans的RDD 转换为DataFrame的BeanInfo，
 *      使用反射得到，定义了表的模式。
 *      目前，Spark SQL不支持包含Map字段的JavaBean 。
 *      但是支持嵌套的JavaBeans和/ List或Array字段。
 *      您可以通过创建实现Serializable的类来创建JavaBean，并为其所有字段设置getter和setter。
 */
public class UsingReflection {
    public static class Person implements Serializable{
        /**
         * Bean
         */
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
                .appName("UsingReflection")
                .getOrCreate();

        /**
         * 读取文本文件
         */
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        //将模式应用于JavaBeans的RDD以获取DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        //将DataFrame注册为临时视图
        peopleDF.createOrReplaceTempView("people");

        //SQL
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        //结果中的行的列可以通过字段索引
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        //查询字段名称
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
        // $example off:schema_inferring$
    }

}
