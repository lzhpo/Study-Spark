package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class PassingFunction {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("PassingFunction")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("file:///E:/Code/LearningBigData/spark-07-rdd/src/File/message.txt");

        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });
        System.out.println(lineLengths);

        //打印行数
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        System.out.println(totalLength);
    }
}
