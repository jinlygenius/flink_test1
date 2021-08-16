package apitest.sink;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/ella/IdeaProjects/flink_test1/src/main/resources/sensor.txt");

        // 从kafka读取数据
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        DataStream<String> inputStream = env.addSource(
//                new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties)
//        );

        // 转成 SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("123.56.4.42:9092", "test-kafka", new SimpleStringSchema()));
//        dataStream.addSink(new FlinkKafkaProducer<String>("localhost:9092", "sensor", new SimpleStringSchema()));
        env.execute();
    }
}
