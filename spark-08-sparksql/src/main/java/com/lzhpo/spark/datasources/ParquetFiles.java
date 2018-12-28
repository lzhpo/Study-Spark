package com.lzhpo.spark.datasources;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class ParquetFiles {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("ParquetFiles")
                .master("local")
                .getOrCreate();
        //读取JSON文件
        Dataset<Row> peopleDF = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");

        //将读取的JSON文件内容写入到file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.parquet
        peopleDF.write().parquet("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.parquet");

        //读入上面创建的Parquet文件。
        // Parquet文件是自描述的，因此保留了模式
        //加载镶木地板文件的结果也是DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.parquet");

        // Parquet文件也可用于创建临时视图，然后再执行SQL语句
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        //结果：
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
    }
}
