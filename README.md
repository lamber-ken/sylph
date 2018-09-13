# Sylph [![Build Status](http://img.shields.io/travis/harbby/sylph.svg?style=flat&branch=master)](https://travis-ci.org/harbby/sylph)
The sylph is stream and batch Job management platform. 
The sylph core idea is to build distributed applications through workflow descriptions.
Support for 
* spark1.x Spark-Streaming
* spark2.x Structured-Streaming 
* flink stream
* batch job

## StreamSql
```sql
create source table topic1(
    key varchar,
    value varchar,
    event_time bigint
) with (
    type = 'ideal.sylph.plugins.flink.source.TestSource'
);

-- Define the data stream output location
create sink table print_table_sink(
    key varchar,
    cnt long,
    window_time varchar
) with (
    type = 'ideal.sylph.plugins.flink.sink.PrintSink',   -- print console
    other = 'demo001'
);

-- Define WATERMARK, usually you should parse the event_time field from the kafka message
create view TABLE foo
WATERMARK event_time FOR rowtime BY ROWMAX_OFFSET(5000)  --event_time Generate time for your real data
AS 
with tb1 as (select * from topic1)  --Usually parsing kafka message here
select * from tb1;

-- Describe the data flow calculation process
insert into print_table_sink
with tb2 as (
    select key,
    count(1),
    cast(TUMBLE_START(rowtime,INTERVAL '5' SECOND) as varchar)|| '-->' 
    || cast(TUMBLE_END(rowtime,INTERVAL '5' SECOND) as varchar) AS window_time
    from foo where key is not null
    group by key,TUMBLE(rowtime,INTERVAL '5' SECOND)
) select * from tb2
```

## UDF UDAF UDTF
The registration of the custom function is consistent with the hive
```sql
create function json_parser as 'ideal.sylph.runner.flink.udf.JsonParser';
```

## Building
sylph builds use Gradle and requires Java 8.
```
# Build and install distributions
gradle clean assemble
```
## Running Sylph in your IDE
After building Sylph for the first time, you can load the project into your IDE and run the server. Me recommend using IntelliJ IDEA.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Sylph makes use of several Java 8 language features
* HADOOP_HOME(2.6.x+) SPARK_HOME(2.3.x+) FLINK_HOME(1.5.x+)

Sylph comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: ideal.sylph.main.SylphMaster
* VM Options: -Dconfig=etc/sylph/sylph.properties -Dlog4j.file=etc/sylph/sylph-log4j.properties
* ENV Options: FLINK_HOME=<your flink home>
               HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
* Working directory: sylph-dist/build
* Use classpath of module: sylph-main
 
## Useful mailing lists
1. yezhixinghai@gmail.com - For discussions about code, design and features

## Help
We need more power to improve the view layer. If you are interested, you can contact me by email.

## Other
<<<<<<< HEAD
* sylph被设计来处理分布式实时ETL,实时StreamSql指标计算,分布式程序监控和托管以及离线周期任务,您可以拿来实验您的方案和思路.如果可以解决您的场景，还是建议您使用
阿里云流计算(https://help.aliyun.com/product/45029.html),
华为流计算等更加成熟可靠的产品来为您的业务保驾护航
* QQ群 438625067
* https://harbby.github.io/project/sylph/index.html


=======
* sylph被设计来处理分布式实时ETL,实时StreamSql计算,分布式程序监控和托管以及离线周期任务.
* 加入QQ群 438625067
>>>>>>> upstream/master
