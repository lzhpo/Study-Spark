package com.lzhpo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《外部数据集》
 *     Spark可以从Hadoop支持的任何存储源创建分布式数据集，
 *     包括本地文件系统，HDFS，Cassandra，HBase，Amazon S3等.Spark支持文本文件，
 *     SequenceFiles和任何其他Hadoop InputFormat。
 */
public class ExternalDatasets {
    public static void main(String[] args) {
        /**
         * 初始化
         */
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("ExternalDatasets");
        JavaSparkContext sc = new JavaSparkContext(conf);


        /**
         * 文本文件RDDS可以使用创建SparkContext的textFile方法。
         * 此方法需要一个URI的文件（本地路径的机器上，或一个hdfs://，s3a://等URI），
         * 并读取其作为行的集合。
         *
         * 创建后，distFile可以通过数据集操作执行操作。
         * 例如，我们可以使用map和reduce操作添加所有行的大小，
         * Eg：distFile.map(s -> s.length()).reduce((a, b) -> a + b)。
         *
         * -有关使用Spark读取文件的一些注意事项：
         *      如果在本地文件系统上使用路径，则还必须可以在工作节点上的相同路径上访问该文件。将文件复制到所有工作者或使用网络安装的共享文件系统。
         *      所有Spark的基于文件的输入方法，包括textFile支持在目录，压缩文件和通配符上运行。例如，你可以使用textFile("/my/directory")，textFile("/my/directory/*.txt")和textFile("/my/directory/*.gz")。
         *      该textFile方法还采用可选的第二个参数来控制文件的分区数。默认情况下，Spark为文件的每个块创建一个分区（HDFS中默认为128MB），但您也可以通过传递更大的值来请求更多的分区。请注意，您不能拥有比块少的分区。
         *
         * -除文本文件外，Spark的Java API还支持其他几种数据格式：
         *      JavaSparkContext.wholeTextFiles允许您读取包含多个小文本文件的目录，并将它们作为（文件名，内容）对返回。这与之相反textFile，每个文件中每行返回一条记录。
         *      对于SequenceFiles，使用SparkContext的sequenceFile[K, V]方法，其中K和V是文件中键和值的类型。这些应该是Hadoop的Writable接口的子类，如IntWritable和Text。
         *      对于其他Hadoop InputFormats，您可以使用该JavaSparkContext.hadoopRDD方法，该方法采用任意JobConf和输入格式类，键类和值类。设置这些与使用输入源的Hadoop作业相同。您还可以使用JavaSparkContext.newAPIHadoopRDD基于“新”MapReduce API（org.apache.hadoop.mapreduce）的InputFormats 。
         *      JavaRDD.saveAsObjectFile并JavaSparkContext.objectFile支持以包含序列化Java对象的简单格式保存RDD。虽然这不像Avro这样的专用格式有效，但它提供了一种保存任何RDD的简便方法。
         */
        //下面这行从外部文件定义基础RDD。此数据集未加载到内存中或以其他方式执行：lines仅仅是指向文件的指针。
        JavaRDD<String> lines = sc.textFile("file:///E:/Code/LearningBigData/spark-07-rdd/src/File/Article.txt");
        //可以验证
        System.out.println("lines--->"+lines);

        //定义lineLengths为map转换的结果。
        //lineLengths不是马上计算，它是属于懒加载。
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());//Lambda表达式
        System.out.println("lineLengths--->"+lineLengths);

        int totalLength = lineLengths.reduce((a, b) -> a + b);//Lambda表达式
        System.out.println("totalLength--->"+totalLength);
        /*
        Article.txt内容为：liuzhaopo
        totalLength的打印结果为：totalLength--->9
         */


        //如果要再次使用lineLengths
        //之前reduce，这将导致lineLengths在第一次计算之后保存在内存中。
        lineLengths.persist(StorageLevel.MEMORY_ONLY());


    }
}
