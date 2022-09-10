package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

//统计窗口中每一个元素对应的个数。这种实现方式是非常的不高效的,需要有其他的方式来实现更加高效的操作的。
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
        long count = 0;
        for (Tuple2<String, Long> in: input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}
