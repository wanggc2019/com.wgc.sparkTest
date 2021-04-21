package com.wgc.sparkExample;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/09/20 星期五 23:34
 */

/**
 * 蒙特卡洛---计算圆周率Pi
 * 数学原理，根据随机选择XY为-1到1的点落在半径为1的圆内的概率
 * http://stackoverflow.com/questions/34892522/the-principle-of-spark-pi
 * 在一个边长为2的正方形内画个圆，正方形的面积 S1=4，圆的半径 r=1，面积 S2=πr^2=π
 * 现在只需要计算出S2就可以知道π，这里取圆心为坐标轴原点，在正方向中不断的随机选点，总共选n个点，
 * 计算在圆内的点的数目为count，则 S2=S1*count/n，然后就出来了。
 *
 * 几个变量的含义：
 * int n ： n是总的取样点数目，这是代码里写明的，取样点选的越多，越精确，当然运算量越大;
 * n是以原点(0,0)为坐标，正向取点（x0，y0）
 * int count ： count是取样点落在圆内的数目，这个数目是根据x * x + y * y <= 1算出来的，原里是
 * 0<x0<1,0<y0<1 => x*x+y*y<=1 满足这个的点就在圆内，不满足的说明在圆外
 * count/n就是圆内点数和圆外（正方形内）点数的比例，这个比例乘以正方形面积就是圆的面积即是pi
 * 参考：https://blog.csdn.net/wuxintdrh/article/details/80401271
 *
 * 脚本：
 * spark2-submit \
 * --master yarn \
 * --deploy-mode cluster \
 * --conf spark.driver.memory=2g \
 * --executor-cores 4 \
 * --class com.wgc.sparkExample.JavaSparkPi JavaSparkPi-1.0-SNAPSHOT.jar
 * */
public class JavaSparkPi {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices; //定义取点数
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < n;i++){
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l,slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1; //0<x0<1
            double y = Math.random() * 2 - 1; //0<y0<1
            return (x * x + y * y <= 1) ? 1 : 0; //判断取点数是否在圆内
        }).reduce(((integer, integer2) -> integer + integer2));

        System.out.println("=====Pi is roughly： " + 4.0 * count / n);

        sparkSession.stop();
    }
}
