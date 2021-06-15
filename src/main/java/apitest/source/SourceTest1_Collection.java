package apitest.source;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1547718199L, 35.8),
                new SensorReading("sensor6", 1547718201L, 15.4),
                new SensorReading("sensor7", 1547718202L, 6.7),
                new SensorReading("sensor10", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 67);

        env.setParallelism(1);

        dataStream.print("data");
        integerDataStream.print("int");

        env.execute();
    }
}
