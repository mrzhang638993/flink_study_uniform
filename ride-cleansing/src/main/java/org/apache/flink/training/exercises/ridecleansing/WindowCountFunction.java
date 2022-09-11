package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WindowCountFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> implements CheckpointedFunction {
    private ListState<Tuple2<String, Integer>> state;
    ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<Tuple2<String, Integer>>("listState", new TupleTypeInfo(Types.STRING(), Types.INT()));
    private List<Tuple2<String, Integer>> bufferedElements = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        //elements代表的是窗口中的所有的元素信息的。
        for (Tuple2<String, Integer> element : elements) {
            out.collect(element);
            state.add(element);
            bufferedElements.add(element);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        //执行快照清除的同事将元素拷贝到state中进行执行
        for (Tuple2<String, Integer> element : bufferedElements) {
            state.add(element);
        }
        bufferedElements.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (context.isRestored()) {
            state = getRuntimeContext().getListState(descriptor);
            //失败重启的时候,从state中拷贝元素到对应的list中。
            for (Tuple2<String, Integer> element : state.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
