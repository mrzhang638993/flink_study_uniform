package org.apache.flink.training.exercises.ridecleansing;

public class IncrementMapFunctionTest {
    public static void main(String[] args) throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();
        // call the methods that you have implemented
        Long map = incrementer.map(2L);
        System.out.println(map==3);
    }
}
