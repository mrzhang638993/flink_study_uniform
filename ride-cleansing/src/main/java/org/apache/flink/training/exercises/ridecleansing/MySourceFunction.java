package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;
import java.util.Random;

//创建source的function的,需要的是基于keyedStream上执行状态保存的
public class MySourceFunction extends RichSourceFunction<Record> implements CheckpointedFunction {
    //定义运行参数,标志是否运行中
    private Boolean isRunning = true;
    private int[] initialValue = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private List<Record> list;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    //代表的是运行中，产生数据结果
    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
        while (isRunning) {
            Record record = new Record();
            Random random = new Random();
            record.setId(initialValue[random.nextInt(10)]);
            record.setTime(System.currentTimeMillis());
            ctx.collect(record);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
