package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《使用reduceByKey键值对上的操作来计算文件中每行文本出现的次数》
 */
public class KeyValue {
    public static void main(String[] args) {

        /**
         * 初始化
         */
        SparkConf conf = new SparkConf()
                .setAppName("KeyValue")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///E:/Code/LearningBigData/spark-07-rdd/src/File/kv.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        System.out.println("pairs--->"+pairs);

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        System.out.println("counts--->"+counts);

        /*
        counts.sortByKey();//按字母排序
        counts.collect();//将它们作为对象数组返回到驱动程序。
        */
        System.out.println(counts.sortByKey());
        System.out.println(counts.collect());

        /**
         * 注意：在键值对操作中使用自定义对象作为键时，必须确保自定义equals()方法附带匹配hashCode()方法。
         */
    }
}
