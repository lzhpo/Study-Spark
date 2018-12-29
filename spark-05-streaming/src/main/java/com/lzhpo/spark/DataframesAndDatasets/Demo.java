package com.lzhpo.spark.DataframesAndDatasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《创建流式DataFrame和流式数据集》
 *      输入源
 *          有一些内置源：
 *
 *              -文件源 - 将目录中写入的文件作为数据流读取。支持的文件格式为text，csv，json，orc，parquet。有关更新的列表，请参阅DataStreamReader接口的文档，以及每种文件格式支持的选项。请注意，文件必须原子放置在给定目录中，在大多数文件系统中，可以通过文件移动操作来实现。
 *
 *              -Kafka来源 - 从Kafka读取数据。它与Kafka经纪人版本0.10.0或更高版本兼容。有关更多详细信息，请参阅Kafka集成指南。
 *
 *              -套接字源（用于测试） - 从套接字连接读取UTF8文本数据。侦听服务器套接字位于驱动程序中。请注意，这应该仅用于测试，因为这不提供端到端的容错保证。
 *
 *              -速率源（用于测试） - 以每秒指定的行数生成数据，每个输出行包含一个timestamp和value。其中timestamp是一个Timestamp含有信息分配的时间类型，并且value是Long包含消息的计数从0开始作为第一行类型。此源用于测试和基准测试。
 */
public class Demo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Demo")
                .master("local")
                .getOrCreate();

        Dataset<Row> socketDF = spark
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",9999)
                .load();

        socketDF.isStreaming();

        socketDF.printSchema();

        StructType userSchema = new StructType().add("name","String").add("age","integer");

        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep",";")
                .schema(userSchema)
                .csv("file:///E:/Code/LearningBigData/spark-05-streaming/src/File/people.csv");
        csvDF.printSchema();
    }
}
