package com.wgc.sparkSimpleExanple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author wanggc
 * @date 2019/09/15 星期日 16:14
 */

/**
 * 这是SparkContext写的
 * 简单的统计文件中某个字符的频次 wordcount
 * */
public class SimpleApp {
    private static String appName = "simpleApp"; //设置Spark应用程序的名称
    //private static String master = "local[*]"; //本地local模式，本地测试，集群测试应注释

    public static void main(String[] args) {

        JavaSparkContext sparkContext = null;
        //String pathName = "E:\\hdfsUpload.txt"; // 本地模式测试
        String pathName = "/user/eda/README.md"; //hdfs
        /**
         * 1、创建SparkConf对象，设置Spark应用程序的配置信息
         */
        try {
            SparkConf conf = new SparkConf()
                    .setAppName(appName);
                    //.setMaster(master);

/*      conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@MYCDH");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@MYCDH");
        conf.set("yarn.resourcemanager.principal","yarn/_HOST@MYCDH");
        conf.set("yarn.nodemanager.principal","yarn/_HOST@MYCDH");
*/

            /**
             * 2、创建SparkContext对象，Java开发使用JavaSparkContext；Scala开发使用SparkContext
             * 在Spark中，SparkContext负责连接Spark集群，创建RDD、累积量和广播量等。
             * Master参数是为了创建TaskSchedule（较低级的调度器，高层次的调度器为DAGSchedule），如下：
             *         如果setMaster("local")则创建LocalSchedule；
             *         如果setMaster("spark")则创建SparkDeploySchedulerBackend。在SparkDeploySchedulerBackend的start函数，会启动一个Client对象，连接到Spark集群。
             */
            sparkContext = new JavaSparkContext(conf);

            /**
             * 3、sc中提供了textFile方法是SparkContext中定义的，如下：
             *         def textFile(path: String): JavaRDD[String] = sc.textFile(path)
             * 用来读取HDFS上的文本文件、集群中节点的本地文本文件或任何支持Hadoop的文件系统上的文本文件，它的返回值是JavaRDD[String]，是文本文件每一行
             */
            JavaRDD<String> logData = sparkContext.textFile(pathName).cache();

            //filter:返回仅包含满足谓词的元素的新RDD
            long numAs = logData.filter(new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    return s.contains("a");
                }
            }).count();

            long numBs = logData.filter(new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    return s.contains("b");
                }
            }).count();

            //打印统计结果
            System.out.println("\n********Lines with a: " + numAs + "\n********lines with b: " + numBs + "\n");
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            //关闭sparkContext
            if (sparkContext != null) {
                sparkContext.stop();
            }
        }
    }
}
