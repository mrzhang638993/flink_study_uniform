package org.apache.flink.training.exercises.ridecleansing;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;


//测试基于eventTime的窗口的触发操作时间的.窗口的结束时间>=eventTime的时间+延时时间，超过这个限制的话，数据会丢失的。
//基于数据延迟存在如下的处理思路的:1.丢弃数据；2.增加延时机制，不建议使用，会导致更多的问题；3.使用sideOutPutstream来处理统计丢弃的数据占比，确保数据的可用性。
public class Test12 {
    //还缺少状态存储,重启策略的配置要素等的。是个很关键的要素的，需要关注。
    public static void main(String[] args) throws Exception {
        //测试能否收集到执行过程中的数据丢失情况？一般的在内网的环境下面是不存在数据丢失的情况的,内网的环境下面可以保证数据的完全的不丢失的。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.registerTypeWithKryoSerializer(SocketPo.class, JavaSerializer.class);
        DataStreamSource<String> sourceStream = env.socketTextStream("10.1.1.1", 9999, "\n");
        SingleOutputStreamOperator<SocketPo> source = sourceStream.map(text -> {
            String[] split = text.split(",");
            SocketPo socketPo = new SocketPo(split[0], Long.valueOf(split[1]));
            return socketPo;
        }).name("socket_source").uid("socket_source")
                .<SocketPo>assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new <SocketPo>TestNewGenerate()).<SocketPo>withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));
        OutputTag<SocketPo> outputTag = new OutputTag<SocketPo>("late_date") {
        };
        //固定时间窗口的话,滑动时间可以设置为0的参数。
        //滚动元素是没有重叠时间的，可以设置固定大小的窗口的。
        //global window对应的是没有窗口的信息的？
        SingleOutputStreamOperator<SocketPo> sideOutput = source.keyBy(po -> po.getKey()).window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .sideOutputLateData(outputTag)//对应的是在处理元素之前的操作的
                .evictor(new MyEvictor())//在窗口计算之前执行元素的剔除操作。
                .trigger(new MyTestTrigger())
                //在窗口计算之前对元素进行过滤操作实现
                .process(new WindowFunction())
                .uid("process_function")
                .name("process_function");
        sideOutput.addSink(new PrintSinkFunction<>());
        sideOutput.getSideOutput(outputTag).addSink(new PrintSinkFunction<>());
        env.execute("test_commom");
    }
}
