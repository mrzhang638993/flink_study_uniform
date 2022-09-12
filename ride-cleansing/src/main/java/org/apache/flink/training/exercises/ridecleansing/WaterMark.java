package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

//创建对应的waterMark
public class WaterMark implements WatermarkGenerator {

    @Override
    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
