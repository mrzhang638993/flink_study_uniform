package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.MapFunction;

class MyMapFunction implements MapFunction<String, Integer> {
    public Integer map(String value) { return Integer.parseInt(value); }
}
