package mypackage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认是cpu核数
        env.setParallelism(8);
//        String inputPath = "/Users/ella/IdeaProjects/test1/src/main/resources/words.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 从socket 文本流读取数据
        // nc -lk 7777
//        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        // 用ParameterTool从程序启动参数中读取
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCountBatch.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }

}
