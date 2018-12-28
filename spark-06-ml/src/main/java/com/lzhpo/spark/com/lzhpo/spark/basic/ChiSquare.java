package com.lzhpo.spark.com.lzhpo.spark.basic;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;


/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 * 《机器学习》---基本统计
 *
 假设检验是统计学中一种强有力的工具，用于确定结果是否具有统计显着性，无论该结果是否偶然发生。
 spark.ml目前支持Pearson的卡方（χ2
 ChiSquareTest对标签的每个功能进行Pearson独立测试。
 对于每个特征，（特征，标签）对被转换为应急矩阵，对其计算卡方统计量。所有标签和特征值必须是分类的。
 *
 */
public class ChiSquare {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("ChiSquare")
      .master("local")
      .getOrCreate();

    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, Vectors.dense(0.5, 10.0)),
      RowFactory.create(0.0, Vectors.dense(1.5, 20.0)),
      RowFactory.create(1.0, Vectors.dense(1.5, 30.0)),
      RowFactory.create(0.0, Vectors.dense(3.5, 30.0)),
      RowFactory.create(0.0, Vectors.dense(3.5, 40.0)),
      RowFactory.create(1.0, Vectors.dense(3.5, 40.0))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Row r = ChiSquareTest.test(df, "features", "label").head();
    System.out.println("pValues: " + r.get(0).toString());
    System.out.println("degreesOfFreedom: " + r.getList(1).toString());
    System.out.println("statistics: " + r.get(2).toString());
    //结果
    /*
    pValues: [0.6872892787909721,0.6822703303362126]
    degreesOfFreedom: [2, 3]
    statistics: [0.75,1.5]
     */


    spark.stop();
  }
}
