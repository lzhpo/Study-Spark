package com.lzhpo.spark.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class SchemaMerging {

    public static class Square implements Serializable {
        private int value;
        private int square;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getSquare() {
            return square;
        }

        public void setSquare(int square) {
            this.square = square;
        }
    }

    public static class Cube implements Serializable {
        private int value;
        private int cube;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SchemaMerging")
                .getOrCreate();


        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        //创建一个简单的DataFrame，存储到分区目录
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        //在新的分区目录中创建另一个DataFrame，
        //添加新列并删除现有列
        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("data/test_table/key=2");

        //读取分区表
        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
        mergedDF.printSchema();

        //最终模式由Parquet文件中的所有3列组成
        //分区列出现在分区目录路径
        // root
        //  |-- value: int (nullable = true)
        //  |-- square: int (nullable = true)
        //  |-- cube: int (nullable = true)
        //  |-- key: int (nullable = true)

    }
}
