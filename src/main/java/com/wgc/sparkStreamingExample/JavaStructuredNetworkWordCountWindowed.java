package com.wgc.sparkStreamingExample;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 * sliding window of configurable duration. Each line from the network is tagged
 * with a timestamp that is used to determine the windows into which it falls.
 * 在可配置持续时间的滑动窗口中，对从网络接收到的以UTF8编码，以'\ n'分隔的文本进行计数。
 * 网络中的每条线都标记有时间戳，该时间戳用于确定其所属的窗口。
 * Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port> <window duration> [<slide duration>]
 * <hostname> and <port> describe the TCP server that Structured Streaming would connect to receive data. 这是待接收数据的主机host和端口
 * <window duration> gives the size of window, specified as integer number of seconds 窗口，一个时间段。系统支持对一个窗口内的数据进行计算，指定为整数秒数
 * <slide duration> gives the amount of time successive windows are offset from one another,given in the same units as above. <slide duration> should be less than or equal to<window duration>.
 * If the two are equal, successive windows have no overlap. If <slide duration> is not provided, it defaults to <window duration>.
 * <slide duration>给出连续窗口彼此偏移的时间量，以与上述相同的单位给出。 <slide duration>应小于或等于<窗口持续时间>。
 * 如果两者相等，则连续窗口不重叠。 如果未提供<slide duration>，则默认为<窗口持续时间>。
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCountWindowed localhost 9999 <window duration in seconds> [<slide duration in seconds>]`
 *
 * One recommended <window duration>, <slide duration> pair is 10, 5
 */

public final class JavaStructuredNetworkWordCountWindowed {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port>" +
                    " <window duration in seconds> [<slide duration in seconds>]");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int windowSize = Integer.parseInt(args[2]);
        //参数必须是3个或者4个，3个参数即不指定slide duration，那么默认slide duration等于window duration，如果指定了那么就返回指定的值
        int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
        // slideSize必须小于window duration
        if (slideSize > windowSize) {
            System.err.println("<slide duration> must be less than or equal to <window duration>");
        }
        String windowDuration = windowSize + " seconds";
        String slideDuration = slideSize + " seconds";

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCountWindowed")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .option("includeTimestamp", true)
                .load();

        // Split the lines into words, retaining固定 timestamps
        Dataset<Row> words = lines.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words.groupBy(functions.window(words.col("timestamp"), windowDuration, slideDuration), words.col("word")).count().orderBy("window");

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}

