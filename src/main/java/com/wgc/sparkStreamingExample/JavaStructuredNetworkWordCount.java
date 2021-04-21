package com.wgc.sparkStreamingExample;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: JavaStructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCount
 *    localhost 9999`
 */

/*
* *计算以UTF8编码，从网络接收的'\n'分隔文本的单词数。
 *
  *用法：JavaStructuredNetworkWordCount <主机名> <端口>
  *  <主机名>和<端口>描述了结构化流将连接以接收数据的TCP服务器。
  *要在本地计算机上运行此程序，您需要先运行Netcat服务器
  *`$ nc -lk 9999`
  *然后运行示例
  *`$ bin / run-example sql.streaming.JavaStructuredNetworkWordCount
  *本地主机9999`
* */
public final class JavaStructuredNetworkWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaStructuredNetworkWordCount <hostname> <port>");
            System.exit(1);
        }

        //待接收Structured Streaming数据的主机ip
        String host = args[0];
        //待接收Structured Streaming数据的主机端口
        int port = Integer.parseInt(args[1]);

        //kerberors
        //System.setProperty("java.security.krb5.conf", "E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\krb5.conf");
        //UserGroupInformation.loginUserFromKeytab("E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\eda.keytab","eda@CHINATELECOM.COM");
        //System.out.println("===UserGroupInformation.getLoginUser(): "+UserGroupInformation.getLoginUser());
        //spark入口
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
/*                .master("yarn")
                .config("yarn.resourcemanager.hostname","dsjpt014041")
                .config("spark.driver.host","134.64.90.43")
                .config("hadoop.security.authentication", "Kerberos")
                .config("spark.yarn.queue","eda")*/
                .getOrCreate();


        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                //.option("host", "dsjpt014041")
                .option("host", host)
                //.option("port", "99999")
                .option("port", port)
                .load();

        // Split the lines into words
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        Dataset wordCounts = words.groupBy("value").count();
        // Start running the query that prints the running counts to the console
        //writeStream 用于将流数据集的内容保存到外部存储中的接口。
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete") //指定如何将流式DataFrame/Dataset的数据写入流式接收器。complete仅将流数据帧/数据集中的新行写入接收器
                .format("console") //指定基础输出数据源。
                .start(); //开始执行流查询，当新数据到达时，它将继续将结果输出到给定路径。

        query.awaitTermination(); //通过query.stop（）或异常等待此查询的终止。
    }
}

