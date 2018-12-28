package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

import static jdk.nashorn.internal.objects.Global.println;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《闭包》
 *      Spark的一个难点是在跨集群执行代码时理解变量和方法的范围和生命周期。
 *      修改其范围之外的变量的RDD操作可能经常引起混淆。
 *      在下面的示例中，我们将查看foreach()用于递增计数器的代码，但其他操作也可能出现类似问题。
 */
public class UnderstandingClosures {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("UnderstandingClosures");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 闭包
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        int counter = 0;
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Wrong: Don't do this!!
        //rdd.foreach(x -> counter += x);

        println("Counter value: " + counter);

        /**
         * 本地与群集模式
         * 上述代码的行为未定义，可能无法按预期工作。为了执行作业，Spark将RDD操作的处理分解为任务，每个任务由执行程序执行。在执行之前，Spark计算任务的关闭。闭包是那些变量和方法，它们必须是可见的，以便执行者在RDD上执行计算（在这种情况下foreach()）。该闭包被序列化并发送给每个执行者。
         *
         * 发送给每个执行程序的闭包内的变量现在是副本，因此，当在函数内引用计数器时foreach，它不再是驱动程序节点上的计数器。驱动程序节点的内存中仍然有一个计数器，但执行程序不再可见！执行程序只能看到序列化闭包中的副本。因此，计数器的最终值仍然为零，因为计数器上的所有操作都引用了序列化闭包内的值。
         *
         * 在本地模式下，在某些情况下，该foreach函数实际上将在与驱动程序相同的JVM中执行，并将引用相同的原始计数器，并且可能实际更新它。
         *
         * 为了确保在这些场景中明确定义的行为，应该使用Accumulator。Spark中的累加器专门用于提供一种机制，用于在跨集群中的工作节点拆分执行时安全地更新变量。本指南的“累加器”部分更详细地讨论了这些内容。
         *
         * 通常，闭包 - 类似循环或本地定义的方法的构造不应该用于改变某些全局状态。Spark没有定义或保证从闭包外部引用的对象的突变行为。执行此操作的某些代码可能在本地模式下工作，但这只是偶然的，并且此类代码在分布式模式下不会按预期运行。如果需要某些全局聚合，请使用累加器。
         *
         * 打印RDD的元素
         * 另一个常见的习惯用法是尝试使用rdd.foreach(println)或打印出RDD的元素rdd.map(println)。在一台机器上，这将生成预期的输出并打印所有RDD的元素。但是，在cluster模式下，stdout执行程序调用的输出现在写入执行stdout程序，而不是驱动程序上的输出，因此stdout驱动程序不会显示这些！要打印驱动程序上的所有元素，可以使用该collect()方法首先将RDD带到驱动程序节点：rdd.collect().foreach(println)。但是，这会导致驱动程序内存不足，因为collect()将整个RDD提取到一台机器上; 如果您只需要打印RDD的一些元素，更安全的方法是使用take()：rdd.take(100).foreach(println)。
         */
    }
}
