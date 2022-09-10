package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySplitEnumerator /*implements SplitEnumerator<MySplit>*/ {
    private final long DISCOVER_INTERVAL = 60_000L;

    /**
     * A method to discover the splits.
     */
    private List<MySplit> discoverSplits(){
        return null;
    };
    //@Override
    public void start() {
        /*enumContext.callAsync(this::discoverSplits, splits -> {
            Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
            int parallelism = enumContext.currentParallelism();
            for (MockSourceSplit split : splits) {
                int owner = split.splitId().hashCode() % parallelism;
                assignments.computeIfAbsent(owner, new ArrayList<>()).add(split);
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignments));
        }, 0L, DISCOVER_INTERVAL);*/
    }

    //@Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    //@Override
    public void addReader(int subtaskId) {

    }

    //@Override
    public Object snapshotState(long checkpointId) throws Exception {
        return null;
    }

    //@Override
    public void close() throws IOException {

    }

    //@Override
    public void addSplitsBack(List splits, int subtaskId) {

    }
}
