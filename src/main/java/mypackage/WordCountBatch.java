package mypackage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {


    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "/Users/ella/IdeaProjects/test1/src/main/resources/words.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 处理数据，按空格分词，展开成 (word, 1) 这种二元组
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置的word统计
                .sum(1);// 按照第二个位置sum
        resultSet.print();

    }


    // 自定义类 实现flatMapper

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格分词，得到String类型的数组
            String[] words = s.split(" ");
            // 遍历所有word，按二元组输出
            for(String word: words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
