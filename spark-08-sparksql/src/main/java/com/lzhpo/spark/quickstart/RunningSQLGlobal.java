package com.lzhpo.spark.quickstart;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《创建全局视图》
 */
public class RunningSQLGlobal {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");

        //将DataFrame注册为全局临时视图
        df.createGlobalTempView("people");

        //全局临时视图绑定到系统保留的数据库`global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        //全局临时视图是跨会话
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
    }
}
