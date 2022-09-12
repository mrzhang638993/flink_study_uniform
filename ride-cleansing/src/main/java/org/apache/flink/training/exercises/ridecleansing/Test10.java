package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test10 {
    public static void main(String[] args) throws Exception {
        //定义执行环境信息
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //定义和使用窗口执行相关的窗口操作和管理,需要进行关注和实现,这个是很重要的特性的。
        //对应的执行环境信息
        env.setParallelism(1);
        env.execute("wordCount");
    }
}
