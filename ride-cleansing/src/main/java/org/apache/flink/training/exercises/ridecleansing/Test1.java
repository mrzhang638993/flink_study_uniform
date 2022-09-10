package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        MapStateDescriptor descriptor = new MapStateDescriptor("test", Types.STRING(), Types.STRING());
        Map<String, String> test1 = new HashMap<>();
        test1.put("key1", "value1");
        test1.put("key2", "value2");
        test1.put("key3", "value3");
        test1.put("key4", "value4");
        SourceFromMySQL sourceFromMySQL = new SourceFromMySQL();
        SingleOutputStreamOperator<Url> source = env.addSource(sourceFromMySQL).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(100000, 100000)));
        //构建一个广播流
        source.keyBy(new KeySelector<Url, Integer>() {
            @Override
            public Integer getKey(Url url) throws Exception {
                return url.getId();
            }
        }).process(new KeyedProcessFunction<Integer, Url, Url>() {
            @Override
            public void processElement(Url value, Context ctx, Collector<Url> out) throws Exception {
                String sql = "select key1,value from key_value";
                ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String key = resultSet.getString(1);
                    String value1 = resultSet.getString(2);
                    mapState.put(key, value1);
                }
                for (Map.Entry<String, String> entry : mapState.entries()) {
                    System.out.println(entry.getKey() + "=====" + entry.getValue());
                }
                System.out.println("=====================");
                if (time == null) {
                    time = System.currentTimeMillis();
                }
                ctx.timerService().registerEventTimeTimer(time + 10);
                out.collect(value);
            }

            private Connection connection;
            private PreparedStatement ps;
            private MapState<String, String> mapState;
            private Long time;
            private boolean isRunning = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                mapState = getRuntimeContext().getMapState(descriptor);
                //定义广播变量的操作,广播变量的话，在后续的每一个算子中都是可以获取得到的,是很关键的特性的。
                List<String> test = getRuntimeContext().getBroadcastVariable("test");
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/crawler?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE", "root", "123456");
                isRunning=true;
            }

            @Override
            public void close() throws Exception {
                if (!isRunning) {
                    super.close();
                    if (ps != null) {
                        ps.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Url> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                Thread.sleep(10000);
                TimerService timerService = ctx.timerService();
                timerService.deleteEventTimeTimer(time);
                String sql = "select key1,value from key_value";
                ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String key = resultSet.getString(1);
                    String value1 = resultSet.getString(2);
                    mapState.put(key, value1);
                }
                System.out.println("=====================trigger=====================");
            }
        }).broadcast(descriptor);
        env.execute("test1");
    }
}
