package com.wgc.sparkSqlExample;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.soundex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/09/22 星期日 17:39
 *
 */
public class JavaSparkSQLExample {
    //系列化，将对象转化为字节流
    public static class Person implements Serializable{
        private String name;
        private int age;

        public String getName(){
            return name;
        }

        public void setName(String name){
            this.name = name;
        }

        public int getAge(){
            return age;
        }

        public void setAge(int age){
            this.age = age;
        }
    }

    /**
     * main()
     * */
    public static void main(String[] args) throws AnalysisException{


        SparkSession sparkSession = null;
        //初始化sparkSession
        sparkSession = SparkSession
                .builder()
                .appName("Java_spark_sql")
                .master("local[2]")
                //.config("", )
                .getOrCreate();

        runBasicDataFrameExample(sparkSession);

        runDatasetCreationExample(sparkSession);

        runInterSchemaExample(sparkSession);

        runProgrammaticSchemaExample(sparkSession);

        sparkSession.stop();

    }

    /**
     * 1、runBasicDataFrameExample 数据集基本操做
     * @param sparkSession
     * @throws AnalysisException
     */
    private static void runBasicDataFrameExample(SparkSession sparkSession) throws AnalysisException {

        /**
         * 数据集操作
         */
        //创建dataFrame
        Dataset<Row> df = sparkSession.read().json("/user/eda/people.json");
        //Dataset<Row> df = sparkSession.read().json("E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\people.json");

        //显示df内容
        System.out.println("=== df.show() ===");
        df.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

        //以树格式打印schema
        System.out.println("=== df.printSchema() ===");
        df.printSchema();
        //root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        //查询指定的name列
        System.out.println("=== df.select(\"name\").show() ===");
        df.select("name").show();
        //+-------+
        //|   name|
        //+-------+
        //|Michael|
        //|   Andy|
        //| Justin|
        //+-------+

        //查询name和age+1
        df.select(col("name"),col("age").plus(1)).show();
        //+-------+---------+
        //|   name|(age + 1)|
        //+-------+---------+
        //|Michael|     null|
        //|   Andy|       31|
        //| Justin|       20|
        //+-------+---------+

        //查询age大于21的人
        df.filter(col("age").gt(21)).show();
        //+---+----+
        //|age|name|
        //+---+----+
        //| 30|Andy|
        //+---+----+

        //分组查询 根据age统计人数
        df.groupBy("age").count().show();
        //+----+-----+
        //| age|count|
        //+----+-----+
        //|  19|    1|
        //|null|    1|
        //|  30|    1|
        //+----+-----+

        /**
         * 以编程方式来查询sql
         */
        //注册临时表、临时视图！临时视图是session级别的,它会随着session的消息而消失
        //createOrReplaceTempView()使用给定名称创建本地临时视图。此临时视图的生命周期与用于创建此数据集的SparkSession绑定在一起。
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = sparkSession.sql("select * from people");
        sqlDF.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

        /**
         * ===全局临时视图===
         * Spark SQL中的临时视图是会话范围的，如果创建它的会话终止，它将消失。
         * 如果您希望拥有一个在所有会话之间共享的临时视图并保持活动状态，直到Spark应用程序终止，您可以创建一个全局临时视图。
         * 全局临时视图与系统保留的数据库global_temp绑定，我们必须使用限定名称来引用它，例如SELECT * FROM global_temp.view1。
         */
        //创建一个全局临时视图对象
        //查询名字为people的全局临时视图
        df.createGlobalTempView("people");

        //全局临时视图与系统保留的数据库“ global_temp”相关联
        sparkSession.sql("select * from global_temp.people").show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

        //全局临时视图是跨会话的
        sparkSession.newSession().sql("select * from global_temp.people").show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

    }

    /**
     * 2、runDatasetCreationExample
     * @param sparkSession
     */
    private static void runDatasetCreationExample(SparkSession sparkSession){

        //创建一个bean类
        Person person = new Person();
        person.setAge(32);
        person.setName("Andy");

        /**
         * ===创建数据集===
         * 数据集与RDD类似，但是，它们不使用Java序列化或Kryo，而是使用专门的编码器来序列化对象以便通过网络进行处理或传输。
         * 虽然编码器和标准序列化都负责将对象转换为字节，但编码器是动态生成的代码，并使用一种格式，允许Spark执行许多操作，
         * 如过滤，排序和散列，而无需将字节反序列化为对象。
         */
        //对java bean进行编码
        //Encoder是编码器，用于将T类型的JVM对象与内部Spark SQL表示形式相互转换。
        //Creates an encoder for Java Bean of type T.
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        //Collections.singletonList()返回仅包含指定对象的不可变列表。返回的列表是可序列化的
        Dataset<Person> javaBeanDS = sparkSession.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();
        //+---+----+
        //|age|name|
        //+---+----+
        //| 32|Andy|
        //+---+----+

        //类编码器中提供了最常见类型的编码器
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1,2,3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
        transformedDS.collect();// Returns [2, 3, 4]

        //DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        //String path = "E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\people.json";
        String path = "/user/eda/people.json";
        Dataset<Person> peopleDS = sparkSession.read().json(path).as(personEncoder);
        peopleDS.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+
    }

    /**
     * 3、runInterSchemaExample
     * @param sparkSession
     * === 与RDD交互 ===
     * Spark SQL支持两种不同的方法将现有RDD转换为数据集。
     *
     * 第一种方法使用反射来推断包含特定类型对象的RDD的模式。 这种基于反射的方法可以提供更简洁的代码.
     *
     * 第二种方法是通过编程接口，允许您构建模式，然后将其应用于现有RDD。
     */
    private static void runInterSchemaExample(SparkSession sparkSession){
        /**
         * === 使用反射模式 ===
         * Spark SQL支持自动将JavaBeans的RDD转换为DataFrame。使用反射获得的BeanInfo定义了表的模式。
         * 目前，Spark SQL不支持包含Map字段的JavaBean。但是支持嵌套的JavaBeans和List或Array字段。
         * 您可以通过创建实现Serializable的类来创建JavaBean，并为其所有字段设置getter和setter。
         */
        //读取外部数据并创建person对象的rdd
        JavaRDD<Person> personJavaRDD = sparkSession.read()
                .textFile("people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        //将schema应用于JavaBean的RDD以获取DataFrame
        Dataset<Row> peopleDF = sparkSession.createDataFrame(personJavaRDD,Person.class);

        //将dataFrame注册为临时视图
        peopleDF.createOrReplaceTempView("people");

        //可以使用spark提供的sql方法运行SQL语句
        Dataset<Row> teenagersDF = sparkSession.sql("select name from people where age between 13 and 19");

        //可以通过字段索引访问结果中一行的列
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder
        );
        teenagerNamesByIndexDF.show();
        //+------------+
        //|       value|
        //+------------+
        //|Name: Justin|
        //+------------+

        //或者一个字段名
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder
        );
        teenagerNamesByFieldDF.show();
        //+------------+
        //|       value|
        //+------------+
        //|Name: Justin|
        //+------------+
    }

    /**
     * === 编程方式模式 ===
     * 如果无法提前定义JavaBean类（例如，记录的结构以字符串形式编码，或者文本数据集将被解析，并且字段将针对不同的用户进行不同的投影），
     * 则可以通过编程方式创建Dataset<Row> 有三个步骤。
     * 1)从原始RDD创建行的RDD;
     * 2)创建由与步骤1中创建的RDD中的行结构匹配的StructType表示的模式。
     * 3)通过SparkSession提供的createDataFrame方法将模式应用于行的RDD。
     * @param sparkSession
     *
     * spark2-submit \
     * --master yarn \
     * --deploy-mode cluster \
     * --conf spark.driver.memory=2g \
     * --executor-cores 4 \
     * --class com.wgc.sparkSqlExample.JavaSparkSQLExample \
     * com.wgc.sparkTest-1.0-SNAPSHOT.jar
     */
    private static void runProgrammaticSchemaExample(SparkSession sparkSession){
        //1)创建一个RDD
        JavaRDD<String> peopleRDD = sparkSession.sparkContext()
                //.textFile("E:\\IdeaProjects\\sparkTest\\src\\main\\resources\\people.txt",1)
                .textFile("/user/eda/people.json",1)
                .toJavaRDD();

        //schema编码为字符串
        String schemaString = "name age";

        //2)创建由与步骤1中创建的RDD中的行结构匹配的StructType表示的模式。
        // 基于字符串格式的schema生成schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")){
            //static StructField	createStructField(String name, DataType dataType, boolean nullable):Creates a StructField with empty metadata.
            StructField field = DataTypes.createStructField(fieldName,DataTypes.StringType,true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        //将RDD（people）的记录转换为行
        JavaRDD<Row> rowJavaRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0],attributes[1].trim());
        });

        //3)通过SparkSession提供的createDataFrame方法将模式应用于行的RDD。
        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);

        //用dataFrame创建一个临时视图
        peopleDataFrame.createOrReplaceTempView("people");

        //可以在使用DataFrames创建的临时视图上运行SQL
        Dataset<Row> results = sparkSession.sql("select * from people");

        //SQL查询的结果是DataFrames，并支持所有正常的RDD操作
        //可以通过字段索引或字段名称来访问结果中的行的列
        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> row.getString(0),Encoders.STRING());
        namesDS.show();
    }



}
