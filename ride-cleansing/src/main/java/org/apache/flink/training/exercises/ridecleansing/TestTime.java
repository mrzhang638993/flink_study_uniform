package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

//测试时间分类,1,EventTime:事件时间；2.IngestionTime 数据进入到flink中的时间；3.ProcessingTime:处理时间
//计算每隔五秒最近10秒出现错误的接口的次数信息?
public class TestTime {
    public static void main(String[] args) throws Exception {
        //定义执行环境信息
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //定义相关的时间信息

        env.execute("wordCount");
    }
}
