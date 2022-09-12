package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

//创建source的function的
public class MySourceFunction implements SourceFunction<Record> , CheckpointedFunction {
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
