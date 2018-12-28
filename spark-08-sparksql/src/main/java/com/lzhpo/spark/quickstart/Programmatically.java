package com.lzhpo.spark.quickstart;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class Programmatically {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Programmatically")
                .getOrCreate();

        //创建一个RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.txt",1)
                .toJavaRDD();

        //schema是字符型
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")){
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType,true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        //将RDD（人）的记录转换为行
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        //将架构应用于RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        //使用DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL可以在使用DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // SQL查询的结果是DataFrames并支持所有正常的RDD操作
//结果中行的列可以通过字段索引或字段名称
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
    }
}
