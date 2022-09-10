package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;

import java.util.LinkedList;
import java.util.List;

public class Test3 {
    //需求:当对应的key的个数等于3个的时候，求解其平均值
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        List<Tuple2<String,Integer>> list = new LinkedList<Tuple2<String,Integer>>();
        list.add(Tuple2.of("key1",3));
        list.add(Tuple2.of("key1",2));
        list.add(Tuple2.of("key1",5));
        list.add(Tuple2.of("key2",4));
        list.add(Tuple2.of("key2",10));
        list.add(Tuple2.of("key2",1));
        Buffer<Tuple2<String, Integer>> tuple2Buffer = JavaConverters.asScalaBuffer(list);
        DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(tuple2Buffer, new TupleTypeInfo(Types.STRING(),Types.INT()));
        KeySelector<Tuple2<String, Integer>, String> keySelector = new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        };
        ValueStateDescriptor valueStateDescriptor=new ValueStateDescriptor("count",Long.class);
        ListStateDescriptor<Long> avg = new ListStateDescriptor<>("avg", Long.class);
        streamOne.keyBy(keySelector, Types.STRING()).sum(1);
        DataStream<Tuple2<String, Integer>> process = streamOne.keyBy(keySelector, Types.STRING()).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private ValueState<Long> valueState;
            private ListState<Long> values;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                valueState=getRuntimeContext().getState(valueStateDescriptor);
                values=getRuntimeContext().getListState(avg);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value1, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                values.add(value1.f1.longValue());
                Long initialValue = valueState.value();
                if (initialValue==null){
                    valueState.update(1L);
                }else {
                    valueState.update(initialValue+1);
                }
                if (valueState.value()==3){
                    Long sum=0L;
                    for (Long aLong : values.get()) {
                        sum+=aLong;
                    }
                    System.out.println("==="+sum/3);
                    //对应的清空状态值进行操作,这个是很关键的数据的。
                    valueState.clear();
                }
                out.collect(value1);
            }
        }, new TupleTypeInfo(Types.STRING()));
        process.print();
        env.execute("avg");
    }
}
