package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Test4 {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        List<Tuple2<String,Integer>> tuple2s=new ArrayList<>();
        tuple2s.add(Tuple2.of("key1",3));
        tuple2s.add(Tuple2.of("key1",5));
        tuple2s.add(Tuple2.of("key1",6));
        tuple2s.add(Tuple2.of("key2",4));
        tuple2s.add(Tuple2.of("key2",7));
        tuple2s.add(Tuple2.of("key2",8));
        DataStreamSource<Tuple2<String, Integer>> tupleDataStream = env.fromCollection(tuple2s);
        SingleOutputStreamOperator<Integer> reduce = tupleDataStream.keyBy(tuple -> tuple.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Integer>() {
            private ReducingState<Integer> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ReducingStateDescriptor descriptor = new ReducingStateDescriptor("reduce", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                }, Integer.class);
                reducingState = getRuntimeContext().getReducingState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out) throws Exception {
                //实现累加求和操作,不需要对应的add操作逻辑实现。
                reducingState.add(value.f1);
                out.collect(reducingState.get());
            }
        }).name("reduce");
        reduce.map(cnt->"===="+cnt).print();
        env.execute("myJob");
    }
}
