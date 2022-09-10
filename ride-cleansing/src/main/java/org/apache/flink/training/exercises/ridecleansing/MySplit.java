package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.connector.source.SourceSplit;

public class MySplit implements SourceSplit {
    @Override
    public String splitId() {
        return "123";
    }
}
