package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 *  统计文字出现次数：
 * 在此示例中，我们使用一些转换来构建被调用的（String，Int）对的数据集，counts然后将其保存到文件中。
 */
public class RDD {
    public static void main(String[] args) {

        //创建SparkConf对象
        SparkConf conf = new SparkConf();
        conf.setAppName("RDD");//类名字
        conf.setMaster("local");//本地模式


        JavaSparkContext sc = new JavaSparkContext(conf);
        //加载需要统计的文本文件
        JavaRDD<String> textFile = sc.textFile("file:///E:/Code/LearningBigData/spark-01-easycode/src/File/RDD/rdd.txt");

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        //文件保存目录out文件夹必须由spark运行的时候自己创建，如果已经存在这个文件夹，需要删除
        counts.saveAsTextFile("E:/Code/LearningBigData/spark-01-easycode/src/File/RDD/out");
    }
}
