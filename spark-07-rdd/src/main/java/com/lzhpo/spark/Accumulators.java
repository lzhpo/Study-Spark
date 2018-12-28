package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class Accumulators {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Accumulators")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        LongAccumulator accum = jsc.sc().longAccumulator();

        jsc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        // ...
        // 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

        accum.value();
        // returns 10
    }
}
