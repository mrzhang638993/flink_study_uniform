package org.apache.flink.training.exercises.ridecleansing;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;

//evict可以实现元素的移除操作。结合实现元素的各种功能新的替代操作实现的,可以实现更加丰富的功能实现。
//globalWindow_trigger 可以实现自定义各种类型的window实现规则的,是很关键的操作的。
//基于state可以实现更多的功能和实现的，globalWindow对应的定义了普通的规则实现的,需要把握的。
public class Test13 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.registerTypeWithKryoSerializer(SocketPo.class, JavaSerializer.class);
        DataStreamSource<String> sourceStream = env.socketTextStream("10.1.1.1", 9999, "\n");
        SingleOutputStreamOperator<SocketPo> source = sourceStream.map(text -> {
            String[] split = text.split(",");
            SocketPo socketPo = new SocketPo(split[0], Long.valueOf(split[1]));
            return socketPo;
        }).name("socket_source_1").uid("socket_source_1")
                .<SocketPo>assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new <SocketPo>TestNewGenerate()).<SocketPo>withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));
        OutputTag<SocketPo> outputTag = new OutputTag<SocketPo>("late_date") {
        };
        //使用全局的window的话，对应的是没有相关的触发器的，需要自定义相关的触发器实现触发操作的
        SingleOutputStreamOperator<SocketPo> sum = source.windowAll(GlobalWindows.create())
                //globalWindow配置触发器实现触发操作实现，很关键的要素操作
                //.trigger(CountTrigger.of(3))
                //自定义触发器实现，计算每隔两个单词计算三个单词的操作，跳跃操作的。使用trigger触发计算，计算完成在evict中剔除三个元素。
                .trigger(new GlobalWindowTrigger())
                .sum(1)
                .uid("sum")
                .name("sum_global");
        sum.addSink(new PrintSinkFunction<>());
        env.execute("global_trigger");
    }
}
