package apitest.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

public class SinkTest_ODPS {
    public static void main(String[] args) throws Exception{
        String access_id = System.getenv("access_id");
        String access_key = System.getenv("access_key");
        String project_name = System.getenv("project_name");
        String endpoint = System.getenv("endpoint");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "123.56.4.42:9092");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test-kafka", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = dataStream.flatMap(new OneToManyFlatmapFunction());

        jsonObjectSingleOutputStreamOperator.addSink(new MySinkFunction(access_id,access_key,project_name,endpoint));
        dataStream.print();
        env.execute();
    }
}
