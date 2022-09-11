package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Test8 {
    public static void main(String[] args) throws Exception {
        //定义执行环境信息
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //定义和使用窗口执行相关的窗口操作和管理,需要进行关注和实现,这个是很重要的特性的。
        //对应的执行环境信息
        env.setParallelism(1);
        List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
        tuple2s.add(Tuple2.of("key1", 3));
        tuple2s.add(Tuple2.of("key1", 5));
        tuple2s.add(Tuple2.of("key1", 6));
        tuple2s.add(Tuple2.of("key2", 4));
        tuple2s.add(Tuple2.of("key2", 7));
        tuple2s.add(Tuple2.of("key2", 8));
        DataStreamSource<Tuple2<String, Integer>> tupleDataStream = env.fromCollection(tuple2s);
        SingleOutputStreamOperator<Tuple2<String, Integer>> count_window = tupleDataStream.keyBy(tuple -> tuple.f0)
                //定义统计窗口操作。窗口一般的包含相关的聚集函数的。
                //窗口中包含了3个元素，1代表的是窗口执行元素的个数，即一个元素触发一次计算。
                .countWindow(3, 3)
                .sum(1)
                .uid("count_window");
        count_window.map(cnt -> "====" + cnt).print();
        env.execute("wordCount");
    }
}
