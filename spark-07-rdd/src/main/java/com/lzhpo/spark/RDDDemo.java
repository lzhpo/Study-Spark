package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 * 《RDD》
 *      Spark围绕弹性分布式数据集（RDD）的概念展开，RDD是可以并行操作的容错的容错集合。
 *      创建RDD有两种方法：并行化 驱动程序中的现有集合，或引用外部存储系统中的数据集，
 *      例如共享文件系统，HDFS，HBase或提供Hadoop InputFormat的任何数据源。
 */
public class RDDDemo {
    public static void main(String[] args) {
        /**
         * Spark程序必须做的第一件事是创建一个JavaSparkContext对象，
         * 该对象告诉Spark如何访问集群。
         * 要创建SparkContext您首先需要构建一个包含有关应用程序信息的SparkConf对象。
         */
        SparkConf conf = new SparkConf()
                .setAppName("RDDDemo")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 1.并行化集合
         *      并行集合通过调用创建JavaSparkContext的parallelize在现有的方法Collection中你的驱动程序。
         *      复制集合的元素以形成可以并行操作的分布式数据集。
         *
         *  可以调用distData.reduce((a, b) -> a + b)添加列表的元素
         */
        //创建包含1-5的并行化集合
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> disData = sc.parallelize(data);
        //打印data
        System.out.println("data--->"+data);
        //打印disdata
        System.out.println("disdata--->"+disData);
    }
}
