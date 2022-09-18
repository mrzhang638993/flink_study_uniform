package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class TestNewGenerate implements WatermarkGenerator<SocketPo> {
    private Long waterMark=0L;
    private Long latest = 5000L;

    @Override
    public void onEvent(SocketPo event, long eventTimestamp, WatermarkOutput output) {
        waterMark = Math.max(event.getTimeStamp(), latest);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(waterMark - latest));
    }
}
