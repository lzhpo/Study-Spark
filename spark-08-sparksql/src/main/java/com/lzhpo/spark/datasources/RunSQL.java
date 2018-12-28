package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class RunSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("RunSQL")
                .getOrCreate();

        /**
         * Run SQL
         *      注意：
         *             这里的是 ` 而不是 '
         *             ` 就是Tab上面的那个键盘打出来的
         */
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/users.parquet`");
        sqlDF.show();
        //结果：
        /*
        +------+--------------+----------------+
        |  name|favorite_color|favorite_numbers|
        +------+--------------+----------------+
        |Alyssa|          null|  [3, 9, 15, 20]|
        |   Ben|           red|              []|
        +------+--------------+----------------+
         */
    }
}
