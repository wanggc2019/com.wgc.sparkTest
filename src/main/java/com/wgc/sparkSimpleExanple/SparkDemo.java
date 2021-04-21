package com.wgc.sparkSimpleExanple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/09/16 星期一 9:08
 */

/**
 * 两种rdd的创建方式
 * 1、并行化集合；2、外部数据集
 * */

//并行化集合
//将一组数值求和
public class SparkDemo {
    private static String appName = "spark_arraySum";
    //private static String master = "local[*]";

    public static void main(String[] args) {
        JavaSparkContext sparkContext = null;

        try {
            //初始化JavaSparkContext
            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName);
                    //.setMaster(master);
            sparkContext = new JavaSparkContext(sparkConf);

            //构造数据源
            List<Integer> data = Arrays.asList(1,2,3,4,5);

            //并行化创建RDD
            JavaRDD<Integer> rdd = sparkContext.parallelize(data);

            //map && reduce
            final Integer result = rdd.map(new Function<Integer, Integer>() {
                public Integer call(Integer integer){
                    return integer;
                }
            }).reduce(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer o1, Integer o2) {
                    return o1 + o2;
                }
            });

            System.out.println("===============执行结果：" + result);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (sparkContext != null){
                sparkContext.close();
            }
        }
    }
}
