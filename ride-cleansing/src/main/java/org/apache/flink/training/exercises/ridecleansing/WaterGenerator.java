package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

//定制水印生成器
public class WaterGenerator implements WatermarkGenerator<Record> {
    private long currentTimeStamp = 0L;

    //获取得到对应的时间戳信息，每次数据到来的时候都会调用的
    @Override
    public void onEvent(Record event, long eventTimestamp, WatermarkOutput output) {
        long time = event.getTime();
        currentTimeStamp = Math.max(time, currentTimeStamp);
    }

    //生成水印标志信息
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        //生成水印标志
        output.emitWatermark(new Watermark(currentTimeStamp));
    }
}
