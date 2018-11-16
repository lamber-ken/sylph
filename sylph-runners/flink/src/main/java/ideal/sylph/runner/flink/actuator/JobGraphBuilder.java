package ideal.sylph.runner.flink.actuator;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

public class JobGraphBuilder {



    public static JobGraph sqlgraph() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "kmaster.bigdata.ly:9092,kslave1.bigdata.ly:9092,kslave2.bigdata.ly:9092,kslave3.bigdata.ly:9092");
        properties.put("group.id", "abc");
        properties.put("max.poll.records", "1");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "true");

        new StreamTableDescriptor(
                tableEnv,
                new Kafka()
                        .version(KafkaValidator.CONNECTOR_VERSION_VALUE_010)
                        .topic("RTC_PROCESS_TEST_SAVEPOINT")
                        .properties(properties)
                        .startFromEarliest())
                .withSchema(new Schema()
                        .field("name", Types.STRING())
                        .field("age", Types.INT()))

                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema()   //使用表的 schema

                )
                .inAppendMode()
                .registerTableSource("tt");


        Table table = tableEnv.sqlQuery("select * from tt");
        tableEnv
                .toRetractStream(table, TypeInformation.of(Row.class))
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " --> " + value.f1);
                        Thread.sleep(10);
                    }
                }).setParallelism(4);


        return env.getStreamGraph().getJobGraph();

    }



    public static JobGraph common() {

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "kmaster.bigdata.ly:9092,kslave1.bigdata.ly:9092,kslave2.bigdata.ly:9092,kslave3.bigdata.ly:9092");
        p.setProperty("group.id", "aa");
        p.setProperty("auto.offset.reset", "latest");
        p.setProperty("enable.auto.commit", "true");
        p.setProperty("auto.commit.interval.ms", "10");

        // environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3000, 3 * 1000));
        //env.enableCheckpointing(50);

        // source
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010("RTC_PROJECT_LOG", new SimpleStringSchema(), p);
        DataStream messageStream = env.addSource(consumer).setParallelism(1);


        messageStream.addSink(new RichSinkFunction<String>() {
            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                Thread.sleep(10000);

                if ("6666".equals(value)) {
                    throw new RuntimeException("3333");
                }


                System.out.println(value);
            }
        });

        return env.getStreamGraph().getJobGraph();
    }
















}
