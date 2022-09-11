package org.apache.flink.training.exercises.ridecleansing;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Test6 {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //parameterTool可以用于传递全局的参数信息
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);
        //true代表使用增量快照机制
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
        tuple2s.add(Tuple2.of("key1", 3));
        tuple2s.add(Tuple2.of("key1", 5));
        tuple2s.add(Tuple2.of("key1", 6));
        tuple2s.add(Tuple2.of("key2", 4));
        tuple2s.add(Tuple2.of("key2", 7));
        tuple2s.add(Tuple2.of("key2", 8));
        DataStreamSource<Tuple2<String, Integer>> tupleDataStream = env.fromCollection(tuple2s);
        SingleOutputStreamOperator<Long> process = tupleDataStream.keyBy(tuple -> tuple.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Long>() {
            //对应的是在整个的taskManager中执行的,对应的每一个taskManager都会执行一次打印操作的，整个需要关注一下。
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                //对应的每一个taskManager都会执行一次的，所以，会存在多次打印的。设置并行度为1的话，只会打印一次数据的
                System.out.println("===="+ JSON.toJSONString(globalJobParameters));
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Long> out) throws Exception {
                out.collect(1L);
            }
        });
        process.print();
        env.execute("jobGraph");
    }
}
