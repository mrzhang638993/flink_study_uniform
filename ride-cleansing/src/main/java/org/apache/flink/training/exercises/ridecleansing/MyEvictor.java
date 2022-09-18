package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

//在窗口计算之前和之后删除不符合要求的元素
public class MyEvictor implements Evictor<SocketPo, TimeWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<SocketPo>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        Iterator<TimestampedValue<SocketPo>> iterator = elements.iterator();
        while (iterator.hasNext()){
            //在执行窗口之前删除不符合要求的元素
            if (iterator.next().getTimestamp()<0){
                iterator.remove();
            }
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<SocketPo>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

    }
}
