package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.TimestampAssigner;

//定义时间抽取器信息
public class CountAssigner implements TimestampAssigner<Record> {
    //element 对应的是元素的，recordTimestamp对应的是eventTime相关的数值的
    @Override
    public long extractTimestamp(Record element, long recordTimestamp) {
        return element.getTime() - recordTimestamp;
    }
}
