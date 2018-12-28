package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class BroadcastVariables {
    public static void main(String[] args) {

        /**
         * 初始化
         */
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("BroadcastVariables");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 广播变量是v通过调用从变量创建的SparkContext.broadcast(v)。
         * 广播变量是一个包装器v，可以通过调用该value 方法来访问它的值。
         */
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});

        broadcastVar.value();
        // returns [1, 2, 3]
    }
}
