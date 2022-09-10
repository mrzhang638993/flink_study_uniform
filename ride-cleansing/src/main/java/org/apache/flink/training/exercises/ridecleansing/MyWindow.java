package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;

//实现自定义的window
public class MyWindow extends WindowAssigner {

    @Override
    public Collection assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        return null;
    }

    @Override
    public Trigger getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public TypeSerializer getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
