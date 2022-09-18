package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFunction extends ProcessWindowFunction<SocketPo, SocketPo, String, TimeWindow> {
    private Counter successCount;
    private Counter failedCount;
    private Long windowMaxTime = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        successCount = getRuntimeContext().getMetricGroup().counter("success_count");
        failedCount = getRuntimeContext().getMetricGroup().counter("failed_count");
    }

    @Override
    public void process(String s, Context context, Iterable<SocketPo> elements, Collector<SocketPo> out) throws Exception {
        for (SocketPo element : elements) {
            successCount.inc();
            windowMaxTime = Math.max(element.getTimeStamp(), windowMaxTime);
            out.collect(element);
        }
        //生成一个数据到OutputStream中去
        //context.output();
    }


}
