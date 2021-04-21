package com.wgc.sparkHiveExample;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/09/19 星期四 0:14
 */

/**
 * 这是用sparkSession来写的
 * 测试spark操做hive表
 * spark-snmbit脚本是：
 * spark2-submit \
 * --master yarn \
 * --deploy-mode cluster \
 * --conf spark.driver.memory=2g \
 * --executor-cores 4 \
 * --class com.wgc.sparkHiveExample.JavaSparkHiveExample \
 * com.wgc.sparkTest-1.0-SNAPSHOT.jar
 * */
public class JavaSparkHiveExample {

    /*
    * Serializable序列化接口
    * 序列化主要作用将类的实例持久化保存，序列化是指把对象转换为字节序列的过程，我们称之为对象的序列化，就是把内存中的这些对象变成一连串的字节(bytes)描述的过程。
    * 反序列化就是把持久化的字节文件数据恢复为对象的过程。
    * 序列化就是保存，反序列化就是读取
    * */
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    //main
    public static void main(String[] args) {

        //hivewarehouse路径
        String hivePath ="hdfs://nameservice1/user/hive/warehouse/eda.db";
        String warehouseLoaction = new File(hivePath).getAbsolutePath();
        SparkSession sparkSession = null;
/*

        System.setProperty("java.security.krb5.conf", "krb5.conf");
        System.setProperty("hive.metastore.sasl.enabled", "true");
        System.setProperty("hive.security.authorization.enabled", "true");
        System.setProperty("hive.metastore.kerberos.principal", fullPrincipal);
        System.setProperty("hive.metastore.execute.setugi", "true");
        System.setProperty("hadoop.security.authentication", "kerberos");
*/

        try {
            sparkSession = SparkSession
                    .builder()
                    //.master("local[*]")
                    .appName("JavaSparkHiveExample")
                    .config("hive.metastore.uris", "thrift://bigdata014230:9083,thrift://bigdata014231:9083")
                    .config("spark.sql.warehouse.dir", warehouseLoaction)
                    //.config("hadoop.security.authentication", "kerberos")
                    //.config("spark.sql.hive.hiveserver2.jdbc.url.principal","hive/_HOST@MYCDH")
                    .enableHiveSupport()
                    .getOrCreate();

            //查询hive sql,显示结果的前20行
            sparkSession.sql("select * from  eda.pokes").show();
            //+---+-------+
            //|foo|    bar|
            //+---+-------+
            //|238|val_238|
            //| 86| val_86|
            //|311|val_311|
            //| 27| val_27|
            //|165|val_165|
            //|409|val_409|
            //|255|val_255|
            //|278|val_278|
            //| 98| val_98|
            //|484|val_484|
            //|265|val_265|
            //|193|val_193|
            //|401|val_401|
            //|150|val_150|
            //|273|val_273|
            //|224|val_224|
            //|369|val_369|
            //| 66| val_66|
            //|128|val_128|
            //|213|val_213|
            //+---+-------+
            //only showing top 20 rows

            //查询统计count
            sparkSession.sql("select count(*) from  eda.pokes").show();
            //+--------+
            //|count(1)|
            //+--------+
            //|     500|
            //+--------+

            //sql()执行的结果是DataFrames类型的
            Dataset<Row> sqlDF = sparkSession.sql("select foo,bar from eda.pokes where foo < 100 order by foo");
            Dataset<String> stringDS = sqlDF.map((MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1), Encoders.STRING());
            ////显示结果的前20行，日志可见
            stringDS.show();
            //+--------------------+
            //|               value|
            //+--------------------+
            //|Key: 0, Value: val_0|
            //|Key: 0, Value: val_0|
            //|Key: 0, Value: val_0|
            //|Key: 2, Value: val_2|
            //|Key: 4, Value: val_4|
            //|Key: 5, Value: val_5|
            //|Key: 5, Value: val_5|
            //|Key: 5, Value: val_5|
            //|Key: 8, Value: val_8|
            //|Key: 9, Value: val_9|
            //|Key: 10, Value: v...|
            //|Key: 11, Value: v...|
            //|Key: 12, Value: v...|
            //|Key: 12, Value: v...|
            //|Key: 15, Value: v...|
            //|Key: 15, Value: v...|
            //|Key: 17, Value: v...|
            //|Key: 18, Value: v...|
            //|Key: 18, Value: v...|
            //|Key: 19, Value: v...|
            //+--------------------+

            //用DataFrames 通过SparkSession创建临时的views
            List<Record> records = new ArrayList<>();
            for (int key = 1; key < 100; key++) {
                Record record = new Record();
                record.setKey(key);
                record.setValue("val_" + key);
                records.add(record);
            }
            Dataset<Row> recordsDF = sparkSession.createDataFrame(records, Record.class);
            recordsDF.createOrReplaceTempView("records");
            //javaRDD
            //public JavaRDD<T> javaRDD()
            //Returns the content of the Dataset as a JavaRDD of Ts.
            //DateSet show():显示sql结果的前20行
            sparkSession.sql("select * from records").show();
            //结果从DateSet转化为javaRDD格式，并保存为一个文件
            //JavaRDD ,static void	saveAsTextFile(String path)
            sparkSession.sql("select * from records").javaRDD().saveAsTextFile("hdfs://nameservice1/user/eda/result1");
            //DataFrameWriter<T>	write()
            //Interface for saving the content of the non-streaming Dataset out into external storage.
            //void	save(String path)
            //Saves the content of the DataFrame at the specified path.
            //将sql执行结果保存在hdfs上
            //保存为test时，只能保存一列，如果有2列结果，报错：org.apache.spark.sql.AnalysisException: Text data source supports only a single column, and you have 2 columns.;
            recordsDF.write().format("json").save("hdfs://nameservice1/user/eda/result2");
        //    +---+------+
            //|key| value|
            //+---+------+
            //|  1| val_1|
            //|  2| val_2|
            //|  3| val_3|
            //|  4| val_4|
            //|  5| val_5|
            //|  6| val_6|
            //|  7| val_7|
            //|  8| val_8|
            //|  9| val_9|
            //| 10|val_10|
            //| 11|val_11|
            //| 12|val_12|
            //| 13|val_13|
            //| 14|val_14|
            //| 15|val_15|
            //| 16|val_16|
            //| 17|val_17|
            //| 18|val_18|
            //| 19|val_19|
            //| 20|val_20|
            //+---+------+
            //only showing top 20 rows
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }
    }
}


