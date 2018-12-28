package com.lzhpo.spark.datasources;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《Hive》
 *      resources配置文件：hive-site.xml，core-site.xml（安全性配置），以及hdfs-site.xml（对于HDFS配置）文件
 *
 */
public class Hive {
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {
        // warehouseLocation points to the default location for managed databases and tables
        /**
         * 如果未配置hive-site.xml，则上下文会自动metastore_db在当前目录中创建并创建由其配置spark.sql.warehouse.dir的目录spark-warehouse
         * 该目录默认为 当前目录中启动Spark应用程序的目录。
         *
         * Spark 2.0.0以来，该hive.metastore.warehouse.dir属性hive-site.xml已被弃用。而是spark.sql.warehouse.dir用于指定仓库中数据库的默认位置。
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("Hive")
                .master("local")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        /**
         * 运行之前最好先测试一下能不能连接到hive再执行下面的一系列操作！！！
         */
//        spark.sql("select * from db_spark.tb_user").show();

        /**
         * 测试成功连接之后，接下来的操作
         */
//        spark.sql("CREATE TABLE IF NOT EXISTS db_spark.tb_src (key INT, value STRING) USING hive");
//        spark.sql("LOAD DATA LOCAL INPATH 'file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/kv1.txt' INTO TABLE db_spark.tb_src");

        //查询db_spark.tb_src数据表
        spark.sql("SELECT * FROM db_spark.tb_src").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        //聚合查询
        spark.sql("SELECT COUNT(*) FROM db_spark.tb_src").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+

        // SQL查询的结果本身就是DataFrames并支持所有正常的功能。
        //查询key<10的根据key排序
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM db_spark.tb_src WHERE key < 10 ORDER BY key");

        // DataFrames中的项目为Row类型，允许您按顺序访问每列。
        Dataset<String> stringsDS = sqlDF.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                Encoders.STRING());
        stringsDS.show();
        // +--------------------+
        // |               value|
        // +--------------------+
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // ...


        //您还可以使用DataFrames在SparkSession中创建临时视图。
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        //然后，查询可以将DataFrames数据与存储在Hive中的数据连接起来。
        spark.sql("SELECT * FROM records r JOIN db_spark.tb_src s ON r.key = s.key").show();
        // +---+------+---+------+
        // |key| value|key| value|
        // +---+------+---+------+
        // |  2| val_2|  2| val_2|
        // |  2| val_2|  2| val_2|
        // |  4| val_4|  4| val_4|
        // ...
    }
}
