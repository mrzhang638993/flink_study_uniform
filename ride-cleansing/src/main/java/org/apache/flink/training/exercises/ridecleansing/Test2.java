package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;

import java.util.LinkedList;
import java.util.List;

//获取累加器的执行结果
public class Test2 {
    public static void main(String[] args) {
        //创建本地的执行的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        List<String> list = new LinkedList<String>();
        list.add("count1");
        list.add("count2");
        list.add("count3");
        Buffer<String> buffer = JavaConverters.asScalaBuffer(list);
        DataStream<String> streamOne = env.fromCollection(buffer, Types.STRING());
        DataStream<Long> counter1 = streamOne.map(new RichMapFunction<String, Long>() {
            private IntCounter intCounter = new IntCounter(0);

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", intCounter);
            }

            @Override
            public Long map(String value) throws Exception {
                intCounter.add(1);
                //获取累加器的本地累加值
                return intCounter.getLocalValue().longValue();
            }
        }, Types.LONG());
        counter1.addSink(new PrintSinkFunction<>());
        JobExecutionResult counter = env.execute("counter");
        Integer  countValue = counter.getAccumulatorResult("counter");
        System.out.println("======"+countValue);
    }
}
