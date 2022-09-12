package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//使用EventTime完成相关的事件生成机制和实现操作
public class Test11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //定义和使用窗口执行相关的窗口操作和管理,需要进行关注和实现,这个是很重要的特性的。
        env.setParallelism(1);
        //定义waterMark生成策略和实现机制
        SingleOutputStreamOperator<Record> operator = env.addSource(new MySourceFunction()).uid("socket_source").name("socket_source")
                //定义WatermarkGenerator 以及对应的TimestampAssigner数据信息
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new WaterGenerator()).withTimestampAssigner(ctx -> new CountAssigner()));
        //对应的执行逻辑
        env.execute("wordCount");
    }
}
