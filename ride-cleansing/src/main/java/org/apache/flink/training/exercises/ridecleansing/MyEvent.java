package org.apache.flink.training.exercises.ridecleansing;

public class MyEvent {
    public boolean hasWatermarkMarker() {
        return true;
    }

    public long getWatermarkTimestamp() {
        return 0L;
    }
}
