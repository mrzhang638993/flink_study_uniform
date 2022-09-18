package org.apache.flink.training.exercises.ridecleansing;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//引入watermark的话，数据还是会重复计算的，使用窗口操作的话，不可避免的导致数据的重复计算操作的。
//使用EventTime完成相关的事件生成机制和实现操作
public class Test11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //定义和使用窗口执行相关的窗口操作和管理,需要进行关注和实现,这个是很重要的特性的。
        env.setParallelism(1);
        //自定义序列化器
        env.getConfig().registerTypeWithKryoSerializer(Record.class, JavaSerializer.class);
        //定义waterMark生成策略和实现机制
        SingleOutputStreamOperator<Record> operator = env.addSource(new MySourceFunction())
                //定义WatermarkGenerator 以及对应的TimestampAssigner数据信息
                .assignTimestampsAndWatermarks (WatermarkStrategy.forGenerator((ctx) -> new WaterGenerator()).withTimestampAssigner(ctx -> new CountAssigner()))
                .name("source_code")
                .uid("source_code");
        //对应的执行逻辑,使用EventTime时间窗口执行操作
        SingleOutputStreamOperator<Record> uid = operator.keyBy(record -> record.getId())
                //滑动时间窗口,10秒钟滑动一次，5秒的滑动间隔。
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("time")
                .name("key_sum")
                .uid("key_sum");
        uid.map(cnt -> cnt).print();
        env.execute("wordCount");
    }
}
