package com.wgc.sparkSimpleExanple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

//参考：http://www.ishenping.com/ArtInfo/3634402.html
/**
 * @author wanggc
 * @date 2019/09/16 星期一 10:42
 */

// 这是用SparkContext写的
//Spark-Java版本WordCount示例

public class WordCount {
    private static String appName = "wordcount";
    private static String master = "local[*]";
    //private static String master = "yarn";

    public static void main(String[] args) {
        /**
         * 1、创建SparkConf对象
         * 设置Spark应用程序的配置信息
         */
        //System.setProperty("java.security.krb5.conf","krb5.conf");
        //System.setProperty("HADOOP_USER_NAME", "wgc");
        //String pathName = "hdfs://134.64.14.37:8020/user/wgc/README.md";
        //String pathName = "E:\\IdeaProjects\\sparkTest\\test.txt";//本地测试
        String pathName = "/user/eda/README.md";
        //String[] jarPath = {"E:\\IdeaProjects\\sparkTest\\target\\com.wgc.sparkTest-1.0-SNAPSHOT.jar"};
        JavaSparkContext javaSparkContext1 = null;

        try {
            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName); //设置Spark应用程序的名称
                    //.setMaster("local[*]")
                    //.setMaster(master) ;// 设置yarn-client模式提交
                    //.set("spark.hadoop.security.authentication", "kerberos")
                    //.set("spark.hadoop.dfs.namenode.kerberos.principal","hdfs/_HOST@MYCDH")
                    //.set("spark.hadoop.yarn.resourcemanager.principal", " yarn/bigdata014233@MYCDH")
                    //.set("yarn.resourcemanager.hostname","dsjpt014037") // 设置resourcemanager的ip
                    //.set("spark.executor.instance","2")  // 设置executor的个数
                    //.set("spark.executor.memory", "1024M")  // 设置executor的内存大小
                    //.set("spark.yarn.queue","wgc")  // 设置提交任务的yarn队列
                    //.set("spark.driver.host","134.64.90.43") // 设置driver的ip地址
                    //.setJars(jarPath); // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开

            /**
             * 2、创建SparkContext对象,SparkContext代表着程序入口
             * java开发使用JavaSparkContext；Scala开发使用SparkContext
             * 在Spark中，SparkContext负责连接Spark集群，创建RDD、累积量和广播量等。
             * Master参数是为了创建TaskSchedule（较低级的调度器，高层次的调度器为DAGSchedule），如下：
             *         如果setMaster("local")则创建LocalSchedule；
             *         如果setMaster("spark")则创建SparkDeploySchedulerBackend。在SparkDeploySchedulerBackend的start函数，会启动一个Client对象，连接到Spark集群。
             */
            javaSparkContext1 = new JavaSparkContext(sparkConf);

            /**
             * 3、读取本地文件
             * sc中提供了textFile方法是SparkContext中定义的，如下：
             *         def textFile(path: String): JavaRDD[String] = sc.textFile(path)
             * 用来读取HDFS上的文本文件、集群中节点的本地文本文件或任何支持Hadoop的文件系统上的文本文件，它的返回值是JavaRDD[String]，是文本文件每一行
             */
            //JavaRDD<String> rdd = javaSparkContext.textFile("E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\test.txt");
            JavaRDD<String> rdd = javaSparkContext1.textFile(pathName);

            /**
             * 4、将行文本内容拆分为多个单词
             * lines调用flatMap这个transformation算子（参数类型是FlatMapFunction接口实现类）返回每一行的每个单词
             */
            JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
                public Iterator<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" ")).iterator(); //每行以空格切割
                }
            });

            /**
             * 5、将每个单词的初始数量都标记为1个,转换为 <word,1>格式
             * words调用mapToPair这个transformation算子（参数类型是PairFunction接口实现类，PairFunction<String, String, Integer>的三个参数是<输入单词, Tuple2的key, Tuple2的value>），返回一个新的RDD，即JavaPairRDD
             */
            JavaPairRDD<String,Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s,1);
                }
            });

            /**
             * 6、统计相同Word的出现频率
             * pairs调用reduceByKey这个transformation算子（参数是Function2接口实现类）对每个key的value进行reduce操作，返回一个JavaPairRDD，这个JavaPairRDD中的每一个Tuple的key是单词、value则是相同单词次数的和
             */
            JavaPairRDD<String,Integer> wordCount = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) {
                    return integer + integer2;
                }
            });

            /**
             * 7、执行action，将结果打印出来
             * 使用foreach这个action算子提交Spark应用程序
             * 在Spark中，每个应用程序都需要transformation算子计算，最终由action算子触发作业提交
             */
            wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                public void call(Tuple2<String, Integer> stringIntegerTuple2) {
                    System.out.println(stringIntegerTuple2._1() + "\t" + stringIntegerTuple2._2());
                }
            });
            //lambda改写
//            wordCount.foreach(s -> System.out.println(s._1() + "\t" + s._2()));
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            /**
             * 8.主动关闭SparkContext
             * */
            if (javaSparkContext1 != null) {
                javaSparkContext1.close();
            }
        }
    }
}
