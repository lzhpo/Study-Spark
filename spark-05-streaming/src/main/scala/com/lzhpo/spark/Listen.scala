package com.lzhpo.spark

import org.apache.spark.sql.SparkSession

/**
  * <p>Create By IntelliJ IDEA</p>
  * <p>Author：lzhpo</p>
  *
  *  《侦听TCP套接字的数据服务器接收的文本数据的运行字数》
  *
  * 如何体验？
  *      Windows命令行输入：nc -lL  -p 9999
  *
  *      之后随意输入数字、字母、汉字之后回车，
  *      然后在idea控制台看效果。
  *
  * 注意：必须使用nc先监听9999端口，不然程序无法运行！
  */

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {

    /**
      *首先，我们必须导入必要的类并创建一个本地SparkSession，
      * 它是与Spark相关的所有功能的起点。
      */
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    /**
      * 接下来，创建一个流数据框，
      * 表示从侦听localhost：9999的服务器接收的文本数据，
      * 并转换DataFrame以计算字数。
      *
      * inesDataFrame表示包含流文本数据的无界表。
      * 此表包含一列名为“value”的字符串，
      * 并且流式文本数据中的每一行都成为表中的一行。
      * 请注意，由于我们只是设置转换，并且尚未启动它，
      * 因此目前没有接收任何数据。接下来，
      * 我们使用了将DataFrame转换为String的数据集.as[String]，
      * 以便我们可以应用flatMap操作将每行拆分为多个单词。
      * 结果words数据集包含所有单词。最后，
      * 我们wordCounts通过对数据集中的唯一值进行分组并对其进行计数来定义DataFrame。
      * 请注意，这是一个流式DataFrame，它表示流的运行字数。
      */
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()


    /**
      * 我们现在已经设置了关于流数据的查询。剩下的就是实际开始接收数据和计算计数。
      * 为此，我们将其设置为outputMode("complete")每次更新时都将完整的计数集（指定者）打印到控制台。然后使用启动流式计算start()。
      */
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
