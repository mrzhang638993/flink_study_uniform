package org.apache.flink.training.exercises.ridecleansing;

import java.util.Objects;

public class SocketPo {
    private String key;
    private Long timeStamp;

    public SocketPo(String key, Long timeStamp) {
        this.key = key;
        this.timeStamp = timeStamp;
    }

    public SocketPo() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SocketPo socketPo = (SocketPo) o;
        return Objects.equals(key, socketPo.key) && Objects.equals(timeStamp, socketPo.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, timeStamp);
    }

    @Override
    public String toString() {
        return "SocketPo{" +
                "key='" + key + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
