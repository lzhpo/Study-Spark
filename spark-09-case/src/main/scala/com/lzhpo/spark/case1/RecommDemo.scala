package com.lzhpo.spark.case1


import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

/**
  *
  * <p>Create By IntelliJ IDEA</p>
  * <p>Author：lzhpo</p>
  *
  * 《ALS用户商品推荐》
  * 一共七款商品，四个用户。
  *       1号用户：
  *           第一个数字是指1号用户
  *           第二个数字是这个用户买了多少款商品
  *           第三个数字是指这个用户的评分
  *               1,0,1.0   --->1号用户买了0号商品，对这个商品的打分是1.0
  *               1,1,2.0   --->1号用户买了1号商品，对这个商品的打分是2.0
  *               1,2,5.0   --->1号用户买了2号商品，对这个商品的打分是5.0
  *               1,3,5.0   --->1号用户买了3号商品，对这个商品的打分是5.0
  *               1,4,5.0   --->1号用户买了4号商品，对这个商品的打分是5.0
  *
  *       2、3、4用户以此类推。
  *
  */
object RecommDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Recommend").setMaster("local[4]")
        val sc = new SparkContext(conf)
        // Load and parse the data
        val data = sc.textFile("file:///E:/Code/LearningBigData/spark-09-case/src/File/data2.txt")
        //变换数据成为Rating。
        val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
            Rating(user.toInt, item.toInt, rate.toDouble)
        })

        // Build the recommendation model using ALS
        val rank = 10
        val numIterations = 10
        //交替最小二乘法算法构建推荐模型
        val model = ALS.train(ratings, rank, numIterations, 0.01)

        // 取出评分数据的(User,product)
//        val usersProducts = ratings.map { case Rating(user, product, rate) =>
//            (user, product)
//        }

        //通过model对(user,product)进行预测,((user, product),rate)
//        val ug2 = sc.makeRDD(Array((2,3),(2,4)))
//        val predictions =
//            model.predict(ug2).map { case Rating(user, product, rate) =>
//                ((user, product), rate)
//            }
        //predictions.collect().foreach(println)
        //1.向用户推荐n款商品
        //val res = model.recommendProducts(5,8);

        //2.将指定的商品推荐给n个用户
        //val res = model.recommendUsers(3,5)

        //3.向所有用户推荐3种商品
        val res = model.recommendProductsForUsers(3)//recommendProducts：自带的推荐商品的方法
        res.foreach(e=>{
            println(e._1 + " ======= ")
            e._2.foreach(println)
        })
        //对训练数据进行map ，((user, product),rate)
//        val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
//            ((user, product), rate)
//        }.join(predictions)
//
//        ratesAndPreds.collect().foreach(println)
//
//        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
//            val err = (r1 - r2)
//            err * err
//        }.mean()
//        println("Mean Squared Error = " + MSE)

//        // Save and load model
//        model.save(sc, "target/tmp/myCollaborativeFilter")
//        val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
        // $example off$
    }
}
