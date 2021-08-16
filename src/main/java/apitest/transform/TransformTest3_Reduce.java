package apitest.transform;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/ella/IdeaProjects/flink_test1/src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
        // KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId); // 方法引用

        // reduce 聚合，取当前最大的温度值，最新的时间戳
        DataStream<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });
        // lambda 写法
        // DataStream<SensorReading> resultStream1 = keyedStream.reduce((curState, newData) -> new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature())));

        resultStream.print();

        env.execute();
    }
}
