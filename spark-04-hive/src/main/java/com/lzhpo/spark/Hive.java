package com.lzhpo.spark;

import org.apache.spark.sql.SparkSession;

/**
 * <p>Create By IntelliJ IDEA</p>
 * <p>Author：lzhpo</p>
 */
public class Hive {
    public static void main(String[] args) {
        SparkSession session = SparkSession
                .builder()
                .appName("Hive")
                .master("local")
                .enableHiveSupport()//开启hive支持
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")//"/user/hive/warehouse"的路径
                .getOrCreate();

        /**
         * 创建表
         */
        session.sql("" +
                "create table db_spark.tb_user" +
                "(" +
                "id int," +
                "name string," +
                "age int," +
                "iphone string" +
                ")" +
                "row format delimited fields terminated by ','" +
                "lines terminated by '\\n'"
        );
        //查询是否创建成功
        session.sql("select * from db_spark.tb_user").show();


        /**
         * 加载数据到创建的表中
         *      -load
         */
        //load加载数据到数据表中
        session.sql("load data inpath '/file/tb_user.txt' into table db_spark.tb_user");
        session.sql("select * from db_spark.tb_user");

        //再次查询数据
        session.sql("select * from db_spark.tb_user").show();


        /**
         *  统计
         *
         */
        session.sql("select count(*) count_num from db_spark.tb_user").show();


        /**
         *  模糊查询
         */
        session.sql("select * from db_spark.tb_user where name like '%小%'").show();



    }
}
