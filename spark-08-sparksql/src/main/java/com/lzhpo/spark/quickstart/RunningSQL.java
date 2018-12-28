package com.lzhpo.spark.quickstart;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class RunningSQL {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("RunningSQL")
                .master("local")
                .getOrCreate();

        //加载json数据
        Dataset<Row> df = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");

        //创建视图，用于sql格式操作
        df.createTempView("people");

        //SQL格式查询
        Dataset<Row> sqlDf = spark.sql("select * from people");
        sqlDf.show();
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
