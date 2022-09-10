package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;

public class Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy
                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                //空闲的数据源指定
                .withIdleness(Duration.ofMinutes(1));
        WatermarkStrategy
                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                //水印对其的概念。
                .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1));
        //kafka基于分区的水印生成机制,可以保证数据的有序的消费操作和实现的。
        /*FlinkKafkaConsumer<MyType> kafkaSource = new FlinkKafkaConsumer<>("myTopic", schema, props);
        kafkaSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        DataStream<MyType> stream = env.addSource(kafkaSource);*/
        /*WatermarkStrategy.forMonotonousTimestamps();
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));*/
        //需要注意的是这里面我们所有的key都是1的,对于不同的key而言是会存在不同的value的。这个需要关注的。
        SingleOutputStreamOperator<Tuple2<Long, Long>> stream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage());
    }
}
