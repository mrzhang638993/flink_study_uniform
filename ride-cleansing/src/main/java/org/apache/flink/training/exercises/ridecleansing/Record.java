package org.apache.flink.training.exercises.ridecleansing;

//进入的Event的记录信息
public class Record {
    private int id;
    private long time;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
