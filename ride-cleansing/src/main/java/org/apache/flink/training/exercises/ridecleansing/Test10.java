package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//使用Eventime的话,对应的需要抽取相关的时间信息？
public class Test10 {
    public static void main(String[] args) throws Exception {
        //定义执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //定义和使用窗口执行相关的窗口操作和管理,需要进行关注和实现,这个是很重要的特性的。
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //定义每隔5秒查询指定范围内10秒的数据,也就是窗口的起始时间是5秒，窗口的范围是10秒钟的时间
        //在source端增加相关的水印时间支持信息。
        SingleOutputStreamOperator<String> socketTextStream = env.addSource(new SocketTextStreamFunction("10.1.1.1", 9999, "\n", 3)).uid("socket_source").name("socket_source")
                //对应的处理的是事件时间信息。
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        //需要严格的注意对应的dataStream的泛型参数支持信息
        SingleOutputStreamOperator<Tuple2<String,Integer>> uid = socketTextStream.map(text -> Tuple2.of(text, 1)).returns(new TupleTypeInfo(Tuple2.class, Types.STRING(), Types.INT())).name("map_transform").uid("map_transform");
        //根据接口来进行区分字段信息。真正的业务场景中可以根据业务线,应用id,接口名称等作为区分字段区分业务实现
        SingleOutputStreamOperator<Tuple2<String, Integer>> interfaceCountSum = uid.keyBy(tuple -> tuple.f0)
                //窗口需要指定对应的时间信息
                //对应的是两秒钟一个的时间窗口的参数参数,对应的是固定时间窗口参数信息。
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(0)))
                .sum(1)
                .uid("interface_count_sum")
                .name("interface_count_sum");
        //这个地方最终是可以落存储的,将数据落入到存储中可以保存数据的。同时我们还要求有特定的应用相关的信息的，做好埋点数据设计是很关键的操作的。
        interfaceCountSum.print();
        env.execute("wordCount");
    }
}
