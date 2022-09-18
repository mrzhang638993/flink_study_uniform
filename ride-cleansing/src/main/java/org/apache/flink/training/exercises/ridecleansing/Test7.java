package org.apache.flink.training.exercises.ridecleansing;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Test7 {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //parameterTool可以用于传递全局的参数信息
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);
        //true代表使用增量快照机制
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        //执行checkpoint机制实现操作,生产环境下面建议使用5分钟的时间间隔的。
        //间隔5秒执行一次checkpoint,对应的是exactly_once的语义的。
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //允许checkpoint超时,当在指定的时间之内,checkpoint没有完成的话,允许超时。后续继续执行下一个checkpoint。
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //允许使用多个线程同时执行checkpoint操作
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //设置检查点checkpoint之间至少存在的间隔时间,防止同时进行checkpoint的操作。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //flink任务出现异常或者是报错的话，state信息的处理机制.RETAIN_ON_CANCELLATION对应的表示的是任务出现异常的话,保存对应的state信息
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //flink任务出现失败的情况下,设置重启策略实现机制。flink任务失败对应的设置重启机制。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        //设置失败率重启机制,根据错误率实现相关的机制。失败率重启机制很少使用,没有必要太多的关注的。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(5000), Time.seconds(5000)));
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
            private ValueState<Long> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                //对应的每一个taskManager都会执行一次的，所以，会存在多次打印的。设置并行度为1的话，只会打印一次数据的
                System.out.println("====" + JSON.toJSONString(globalJobParameters));
                ValueStateDescriptor descriptor = new ValueStateDescriptor("value", Types.LONG());
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5000))
                        .cleanupFullSnapshot()
                        .cleanupIncrementally(1000, true)
                        .cleanupInRocksdbCompactFilter(1000)
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setTtl(Time.seconds(5000))
                        .build();
                //可以配置ttl的过期时间配置的。
                descriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Long> out) throws Exception {
                out.collect(1L);
                ctx.timerService().registerEventTimeTimer(valueState.value() + 5000);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                //触发器触发数据操作的话，这样的话，可以从state中获取得到相关的状态信息。执行接口的调用操作和实现处理逻辑。
            }
        });
        process.print();
        env.execute("jobGraph");
    }
}
