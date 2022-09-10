package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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

public class Test5 {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
        tuple2s.add(Tuple2.of("key1", 3));
        tuple2s.add(Tuple2.of("key1", 5));
        tuple2s.add(Tuple2.of("key1", 6));
        tuple2s.add(Tuple2.of("key2", 4));
        tuple2s.add(Tuple2.of("key2", 7));
        tuple2s.add(Tuple2.of("key2", 8));
        DataStreamSource<Tuple2<String, Integer>> tupleDataStream = env.fromCollection(tuple2s);
        SingleOutputStreamOperator<Integer> aggregate = tupleDataStream.keyBy(tuple -> tuple.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Integer>() {
            private AggregatingState<Tuple2<String, Integer>, Integer> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                AggregatingStateDescriptor descriptor = new AggregatingStateDescriptor("aggregate", new AggregateFunction<Tuple2<String, Integer>, IntCounter, Integer>() {
                    @Override
                    public IntCounter createAccumulator() {
                        return new IntCounter(0);
                    }

                    @Override
                    public IntCounter add(Tuple2<String, Integer> value, IntCounter accumulator) {
                        return new IntCounter(accumulator.getLocalValue() + value.f1);
                    }

                    @Override
                    public Integer getResult(IntCounter accumulator) {
                        return accumulator.getLocalValue();
                    }

                    @Override
                    public IntCounter merge(IntCounter a, IntCounter b) {
                        return new IntCounter(a.getLocalValue() + b.getLocalValue());
                    }
                }, Integer.class);
                aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out) throws Exception {
                aggregatingState.add(value);
                out.collect(aggregatingState.get());
            }
        });
        aggregate.map(cnt->"====="+cnt).print();
        env.execute("aggregate");
    }
}
