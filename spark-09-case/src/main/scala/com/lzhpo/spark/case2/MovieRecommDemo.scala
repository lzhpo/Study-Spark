package com.lzhpo.spark.case2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * <p>Create By IntelliJ IDEA</p>
  * <p>Author：lzhpo</p>
  *
  * 《ALS电影推荐》
  *
  *
  *         0::2::3::1424380312     --->0号用户观看2号电影，评分为3，1424380312为时间戳。
  *         0::3::1::1424380312             ........
  *         0::5::2::1424380312             以此类推
  *         0::9::4::1424380312             ........
  *
  */
object MovieRecommDemo {

    //定义评级样例类
    case class Rating0(userId: Int, movieId: Int, rating: Float, timestamp: Long)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        conf.setAppName("movieRecomm");
        conf.setMaster("local[4]")

        val spark = SparkSession.builder().config(conf).getOrCreate() ;
        import spark.implicits._


        //解析评级
        def parseRating(str: String): Rating0 = {
            val fields = str.split("::")
            assert(fields.size == 4)
            Rating0(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
        }
        //转换成Rating的DF对象
        var ratings = spark.sparkContext.textFile("file:///E:/Code/LearningBigData/spark-09-case/src/File/sample_movielens_ratings.txt");
        val ratings0 = ratings.map(parseRating)
        val df = ratings0.toDF()
        //随机切割训练数据，生成两个一个数组，第一个元素是training,第二个是test
        val Array(training, test) = df.randomSplit(Array(0.99, 0.01))

        //建ALS推荐算法并设置参数
        val als = new ALS().setMaxIter(5)
          .setRegParam(0.01)
          .setUserCol("userId")
          .setItemCol("movieId")
          .setRatingCol("rating")
        //通过als对象对训练数据进行拟合,生成推荐模型
        val model = als.fit(training)
        //使用model对test数据进行变换，实现预测过程
        val predictions = model.transform(test);

        predictions.collect().foreach(println)

    }
}
