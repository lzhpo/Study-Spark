package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《加载CSV格式文件》
 */
public class LoadCSV {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("LoadCSV")
                .master("local")
                .getOrCreate();

        //加载CSV文件
        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep",";")
                .option("inferSchema","true")
                .option("header","true")
                .load("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.csv");
        peopleDFCsv.show();
    }
}
