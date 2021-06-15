package apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties)
        );

        dataStream.print();
        env.execute();
    }
}
