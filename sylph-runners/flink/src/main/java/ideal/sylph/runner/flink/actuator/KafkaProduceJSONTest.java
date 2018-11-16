package ideal.sylph.runner.flink.actuator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProduceJSONTest {


    //--topic test-cql-kafka-inputs --bootstrap.servers 10.100.158.200:9092 --zookeeper.connect 10.100.156.92:2187 --group.id rrr

    public static void main(String[] args) throws Exception {

        System.out.println("begin produce");
        connectionKafka();
        System.out.println("finish produce");
    }

    public static void connectionKafka() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kmaster.bigdata.ly:9092,kslave2.bigdata.ly:9092,kslave3.bigdata.ly:9092,kslave4.bigdata.ly:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);

        for (int i = 0; i < 150; i++) {
            producer.send(new ProducerRecord("RTC_PROCESS_TEST_SAVEPOINT", "{\n" +
                    "\t\"name\": \"ab"+i+"c\",\n" +
                    "\t\"age\": 123\n" +
                    "}"));
            System.out.println(i);
            Thread.sleep(500);
        }

        producer.flush();
        producer.close();
    }
}