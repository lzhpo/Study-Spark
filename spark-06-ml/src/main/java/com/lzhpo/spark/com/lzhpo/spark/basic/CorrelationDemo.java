package com.lzhpo.spark.com.lzhpo.spark.basic;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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
 *      ---《关联》
 *          计算两个数据系列之间的相关性是统计学中的常见操作。
 *          在spark.ml提供的灵活性来计算多个系列两两之间的相关性。
 *          支持的相关方法目前是Pearson和Spearman的相关性。
 *
 *  Correlation 使用指定的方法计算输入数据集的相关矩阵。输出将是一个DataFrame，它包含向量列的相关矩阵。
 *
 */
public class CorrelationDemo {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("CorrelationDemo")
      .master("local")
      .getOrCreate();

    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
      RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
      RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
      RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Row r1 = org.apache.spark.ml.stat.Correlation.corr(df, "features").head();
    System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());
    //结果：
    /*
    Pearson correlation matrix:
    1.0                   0.055641488407465814  NaN  0.4004714203168137
    0.055641488407465814  1.0                   NaN  0.9135958615342522
    NaN                   NaN                   1.0  NaN
    0.4004714203168137    0.9135958615342522    NaN  1.0
    */

    Row r2 = org.apache.spark.ml.stat.Correlation.corr(df, "features", "spearman").head();
    System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
    //结果：
    /*
    Spearman correlation matrix:
    1.0                  0.10540925533894532  NaN  0.40000000000000174
    0.10540925533894532  1.0                  NaN  0.9486832980505141
    NaN                  NaN                  1.0  NaN
    0.40000000000000174  0.9486832980505141   NaN  1.0
     */

    spark.stop();
  }
}
