package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//对应的滚动窗口之内的join操作和实现机制,需要特别的关注和实现操作
public class Test14 {
    public static void main(String[] args) {
        //flink join的时候数据丢失的问题怎么处理？flink的join都是基于窗口的join实现的,如果不是在同一个窗口的话，数据会无法join的？
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    }
}
