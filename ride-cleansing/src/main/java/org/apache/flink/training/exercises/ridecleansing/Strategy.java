package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.*;

//创建watermark生成策略
public class Strategy implements TimestampAssignerSupplier {
    @Override
    public TimestampAssigner createTimestampAssigner(Context context) {
        return null;
    }
}
