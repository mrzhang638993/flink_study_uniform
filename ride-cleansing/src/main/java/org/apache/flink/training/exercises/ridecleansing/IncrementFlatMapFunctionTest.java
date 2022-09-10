package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.util.Collector;

public class IncrementFlatMapFunctionTest {

    public static void main(String[] args) {
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();
        //Collector<Integer> collector = mock(Collector.class);
        // call the methods that you have implemented
        //incrementer.flatMap(2L, collector);
        //verify collector was called with the right output
        //Mockito.verify(collector, times(1)).collect(3L);
    }
}
