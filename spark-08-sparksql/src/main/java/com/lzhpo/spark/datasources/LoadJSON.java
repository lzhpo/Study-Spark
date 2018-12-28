package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《手动指定选项》
 *      您还可以手动指定将要使用的数据源以及要传递给数据源的任何其他选项。
 *      数据源通过其全名指定（即org.apache.spark.sql.parquet），
 *      但内置的来源，你也可以使用自己的短名称（json，parquet，jdbc，orc，libsvm，csv，text）。
 *      从任何数据源类型加载的DataFrame都可以使用此语法转换为其他类型。
 */
public class LoadJSON {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("LoadJSON")
                .getOrCreate();
        Dataset<Row> peopleDF =
                spark.read().format("json").load("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/JSONnamesAndAges.parquet");
        /*
        {
          "type" : "struct",
          "fields" : [ {
            "name" : "name",
            "type" : "string",
            "nullable" : true,
            "metadata" : { }
          }, {
            "name" : "age",
            "type" : "long",
            "nullable" : true,
            "metadata" : { }
          } ]
        }
        and corresponding Parquet message type:
        message spark_schema {
          optional binary name (UTF8);
          optional int64 age;
        }
         */
    }
}
