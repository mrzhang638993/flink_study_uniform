package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

public class MyRichFunction implements RichFunction {
    @Override
    public void open(Configuration parameters) throws Exception {
        //定义state的失效配置信息。对应的是StateTtlConfig配置属性的。
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                /***
                 * StateTtlConfig.UpdateType.OnCreateAndWrite 对应的在state在创建的时候以及写的时候执行更新操作的。
                 * StateTtlConfig.UpdateType.OnReadAndWrit 在状态读取的时候也是会对应的更新状态信息的。需要注意的是当设置OnReadAndWrit的
                 * 时候,如果同时设置了可见性级别为StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp的话，会导致reda的cache被丢弃的
                 * 这个时候会导致性能缺失的，需要关注。
                 * StateTtlConfig.StateVisibility.NeverReturnExpired 可见性，不返回失效的数据。
                 * StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp 返回失效的没有被清理的状态数据。
                 * 状态后端对应的存储了上次修改的时间戳信息。
                 * 需要注意的是state的ttl操作会增加额外的存储空间的,同时state的失效会导致后期的状态恢复存在问题的。
                 * state的ttl的配置不是checkpoint或者是savepoint的一项配置的。仅仅是当前job的配置的。
                 * 对于失效的state而言，在使用value获取state的时候，或者是使用垃圾回收机制的时候，对应的是会执行垃圾回收的。
                 * 可以禁用这样的配置信息。
                 * 可以在执行全局的state的snapshot的时候执行state的清除操作,降低快照的存储空间的。
                 *
                 * 需要注意如下的事项：
                 * 1.失效的状态不在被访问的时候，是会被持久化的；
                 * 2.state的增量清除操作,会增加数据处理的延迟；
                 * 3.增量快照是在堆中进行的，和rocketdb没有关系的；
                 * 4.同步的state快照会增加内存的消耗的,异步快照则没有这样的问题的。
                 * 5.使用RocksDB state backend的话,flink会异步的执行压缩过滤的操作的，会执行异步压缩的方式来合并状态的更新和减少存储空间的。
                 * flink的异步压缩过滤操作会检查对应的时间戳，并排除失效的state实体的。
                 * 那么对于特定的数据的话，我们可以将对应的失效的状态进行设置失效的时间的，从而减少存储的使用的。
                 * 需要注意的是non-keyed的state才可以分享的，keyed-state对应的是不行的。
                 * 广播状态是一个典型的non-keyed状态的，是可以分享的。
                 * Each parallel instance of the Kafka consumer maintains a map of topic partitions and offsets as its Operator State.
                 * kafka的consumer中维护了特定的map的数据结构，对应的结构中包含了topic的分区信息以及对应的offsets的偏移量信息。
                 */
                .cleanupInRocksdbCompactFilter(1000)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                //禁用state的失效清除机制。在执行全部的状态清除的时候，清理失效的状态快照数据。
                .disableCleanupInBackground()
                .build();
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
        stateDescriptor.enableTimeToLive(ttlConfig);
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return null;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {

    }
}
