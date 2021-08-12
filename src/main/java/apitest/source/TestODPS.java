package apitest.source;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class TestODPS {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "123.56.4.42:9092");


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test-kafka", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        dataStream.print();
        env.execute();

        // write to odps
        Properties config = new Properties();
        config.put("access_id", "");
        config.put("access_key", "");
        config.put("project_name", "sparrow_pro_dev");
        // config.put("charset", "...");
        Connection conn = DriverManager.getConnection("jdbc:odps:http://service.cn-beijing.maxcompute.aliyun.com/api", config);
        Statement stmt = conn.createStatement();
        // kafka message is :
        // {"backend_app_id":"app_1521010788","hidden_user_id":"1568299839089236","logs":"[{\"timestamp\":\"1628672162\",\"content\":\"802\",\"action\":\"go_to_cuthand\"}]","share_param":"","source":"wechat_app","timestamp":"1628672164","user":"13811182290","user_id":"1feb5694fca24d3885b51b8b3364a1e5","wechat_app_id":""}
        ResultSet rs = stmt.executeQuery("insert into sparrow_log_frontlog ...");
        // 字段见下方
        /**
        +------------------------------------------------------------------------------------+
                | Field           | Type       | Label | Comment                                     |
                +------------------------------------------------------------------------------------+
                | id              | bigint     |       |                                             |
                | user            | string     |       |                                             |
                | source          | string     |       |                                             |
                | action          | string     |       |                                             |
                | path            | string     |       |                                             |
                | content         | string     |       |                                             |
                | created_time    | datetime   |       |                                             |
                | time_stamp      | bigint     |       |                                             |
                | share_param     | string     |       |                                             |
                | wechat_app_id   | string     |       |                                             |
                | hidden_user_id  | string     |       |                                             |
                | backend_app_id  | string     |       |                                             |
                | user_id         | string     |       |                                             |
                | relation        | string     |       |                                             |
                | version         | string     |       |                                             |
                | front_version   | string     |       |                                             |
                | detail          | string     |       |                                             |
        +------------------------------------------------------------------------------------+
                | Partition Columns:                                                                 |
        +------------------------------------------------------------------------------------+
                | dt              | string     | 分区                                                  |
                +------------------------------------------------------------------------------------+
         */
    }
}



