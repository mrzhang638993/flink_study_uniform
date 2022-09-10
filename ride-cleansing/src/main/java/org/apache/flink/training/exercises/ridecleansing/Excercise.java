package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Excercise {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        /*ExecutionConfig config = executionEnvironment.getConfig();
        //设置水印产生的时间间隔
        config.setAutoWatermarkInterval(100);
        //配置数据刷新到缓冲区的时间间隔。默认是100，设置为5可以增加刷新的次数的，降低数据处理的延时的。
        executionEnvironment.setBufferTimeout(5);
        WatermarkStrategy
                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofMinutes(1));
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
        stateDescriptor.enableTimeToLive(ttlConfig);;*/
        List values=new ArrayList<String>();
        values.add("1");
        values.add("2");
        values.add("3");
        DataStreamSource source = executionEnvironment.fromCollection(values);
        source.shuffle();
    }
}
