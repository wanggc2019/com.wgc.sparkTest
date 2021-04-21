package com.wgc.sparkSimpleExanple;

/**
 * @author wanggc
 * @date 2019/09/16 星期一 9:50
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 两种rdd的创建方式
 * 1、并行化集合；2、外部数据集
 * */

//外部数据集
//读取txt文件，计算包含【spark】的每一行字符长度之和
public class SparkDemo2 {
    private static String appName = "spark_stringSum";
    //private static String master = "local[*]";

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = null;
        //String pathName = "E:\\IdeaProjects\\sparkTest\\test.txt";
        String pathName = "/user/eda/README.md";

        try {
            //初始化 javasparkcontext
            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName);
                    //.setMaster(master);
            javaSparkContext = new JavaSparkContext(sparkConf);

            //从text.txt文件构建rdd
            JavaRDD<String> rdd = javaSparkContext.textFile(pathName);

            //过滤
            rdd = rdd.filter(new Function<String, Boolean>() {
                public Boolean call(String s) {
                    return s.contains("Spark");
                }
            });

            // map && reduce
            Integer result = rdd.map(new Function<String, Integer>() {
                public Integer call(String s) {
                    return s.length();
                }
            }).reduce(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer o1, Integer o2) {
                    return o1 + o2;
                }
            });

            System.out.println("==============执行结果：" + result);

        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (javaSparkContext != null){
                javaSparkContext.close();
            }
        }
    }
}
