package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《通用加载/保存功能》
 *      在最简单的形式中，默认数据源（parquet除非另有配置 spark.sql.sources.default）将用于所有操作。
 */
public class LoadAndSave {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("LoadAndSave")
                .master("local")
                .getOrCreate();

        Dataset<Row> usersDF = spark.read().load("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/users.parquet");
        usersDF.select("name", "favorite_color").write().save("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/namesAndFavColors.parquet");
        //结果：
        /*
        {
          "type" : "struct",
          "fields" : [ {
            "name" : "name",
            "type" : "string",
            "nullable" : true,
            "metadata" : { }
          }, {
            "name" : "favorite_color",
            "type" : "string",
            "nullable" : true,
            "metadata" : { }
          } ]
        }
        and corresponding Parquet message type:
        message spark_schema {
          optional binary name (UTF8);
          optional binary favorite_color (UTF8);
        }
         */
    }
}
