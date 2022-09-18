package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//自定义触发器策略和实现
public class MyTestTrigger extends Trigger<SocketPo, TimeWindow> {

    @Override
    public TriggerResult onElement(SocketPo element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        //执行相关的清理操作实现
    }
}
