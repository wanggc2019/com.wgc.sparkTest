package com.wgc.sparkExample;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author wanggc
 * @date 2019/09/18 星期三 17:44
 */

/**
 * 这是sparkSession写的
 * java版本的spark wordcount程序
 * spark-submit脚本是:(读取hdfs路径)
 * spark2-submit \
 * --master yarn \
 * --deploy-mode cluster \
 * --conf spark.driver.memory=2g \
 * --executor-cores 4 \
 * --class com.wgc.sparkExample.JavaWordCount JavaWordCount-1.0-SNAPSHOT.jar /user/eda/README.md
 * */

public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile( " ");
    private static String appName = "SparkSeeeion_WordCount";
    //private static String master = "local[*]"; //本地测试

    public static void main(String[] args) {

        SparkSession sparkSession = null;

        if (args.length < 1){
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        try {
            sparkSession = SparkSession
                    .builder()
                    .appName(appName)
                    //.master(master)
                    .getOrCreate();

            /**
             * s -> Arrays.asList(SPACE.split(s)).iterator() 这是Lambda表达式入门，java8开始引入
             * 1、s是参数：
             * 一个 Lambda 表达式可以有零个或多个参数；参数的类型既可以明确声明，也可以根据上下文来推断
             * 所有参数需包含在圆括号内，参数之间用逗号相隔，例如(a, b) 或 (int a, int b)；
             * 空圆括号()代表参数集为空。如：() -> 42
             * 当只有一个参数，且其类型可推导时，圆括号（）可省略。例如：a -> return a*a
             * 2、Arrays.asList(SPACE.split(s)).iterator()是表达式主体
             * Lambda 表达式的主体可包含零条或多条语句；
             * 如果 Lambda 表达式的主体只有一条语句，花括号{}可省略。匿名函数的返回类型与该主体表达式一致；
             * 如果 Lambda 表达式的主体包含一条以上语句，则表达式必须包含在花括号{}中（形成代码块）。
             * 匿名函数的返回类型与代码块的返回类型一致，若没有返回则为空
             * 参考：https://blog.csdn.net/ZYC88888/article/details/82622137
             * */
            //JavaRDD<String> lines = sparkSession.read().textFile(args[0]).javaRDD();
            // 在spark-sumbit带上要做wordcount的文件路径放到class后面
            JavaRDD<String> lines = sparkSession.read().textFile(args[0]).javaRDD();
            //可能会报错,要设置idea使用Lambda表达式.
            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

            JavaPairRDD<String,Integer> ones = words.mapToPair(s -> new Tuple2<>(s,1));
            JavaPairRDD<String,Integer> counts = ones.reduceByKey((i1,i2) -> i1 + i2);

            List<Tuple2<String,Integer>> output = counts.collect();
            for (Tuple2<?,?> tuple2 : output){
                System.out.println(tuple2._1() + ": " + tuple2._2());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }
    }
}
