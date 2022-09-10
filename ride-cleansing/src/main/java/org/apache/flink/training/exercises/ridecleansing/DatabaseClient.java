package org.apache.flink.training.exercises.ridecleansing;

import java.util.concurrent.Future;

public class DatabaseClient {
    private String host;
    private int port;
    private String credentials;
    public DatabaseClient(String host,int port,String credentials){
        this.host=host;
        this.port=port;
        this.credentials=credentials;
    }
    public void close() {
    }

    public Future<String> query(String key) {
        return null;
    }
}
