package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

//可以在trigger中使用state吗？
//trigger中可以注册触发器以及对应的state的信息的。registerProcessingTimeTimer registerEventTimeTimer 对应的还有 getPartitionedState
//至于能否使用对应的state的话,查看相关的context中是否支持有对应的state的操作接口
public class GlobalWindowTrigger extends Trigger<SocketPo, GlobalWindow> {
    private ValueState<Long> partitionedState;
    private ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor("count", Long.class);


    @Override
    public TriggerResult onElement(SocketPo element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        //注册window中的最大时间
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        //获取得到基于window的state的数据的
        partitionedState = ctx.getPartitionedState(descriptor);
        Long value = partitionedState.value();
        partitionedState.update(value + 1);
        if (partitionedState.value() >= 2) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

    }
}
