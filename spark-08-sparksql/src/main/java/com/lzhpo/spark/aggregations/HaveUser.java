package com.lzhpo.spark.aggregations;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;


/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 *
 * 《类型安全的用户定义聚合函数》
 *      强类型数据集的用户定义聚合围绕Aggregator抽象类。
 */
public class HaveUser {

    public static class Employee implements Serializable {
        private String name;
        private long salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }

    }

    public static class Average implements Serializable  {
        private long sum;
        private long count;

        public Average() {
        }

        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {
        //此聚合的零值。应满足任何b +0 = b
        public Average zero() {
            return new Average(0L, 0L);
        }
        //组合两个值以生成新值。为了提高性能，该函数可以修改`buffer`
        //并返回它而不是构造一个新对象
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }
        //合并两个中间值
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }
        //变换还原的输出
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }
        //为中间值类型
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }
        //为最终输出值类型指定编码器
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("HaveUser")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "file:///E:/Code/LearningBigData/spark-08-sparksql/src/File/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();
        // +-------+------+
        // |   name|salary|
        // +-------+------+
        // |Michael|  3000|
        // |   Andy|  4500|
        // | Justin|  3500|
        // |  Berta|  4000|
        // +-------+------+

        MyAverage myAverage = new MyAverage();
        //将函数转换为`TypedColumn`并为其命名
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();
        // +--------------+
        // |average_salary|
        // +--------------+
        // |        3750.0|
        // +--------------+
        spark.stop();
    }

}
