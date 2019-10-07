package com.edayqe.learn.flink.app;

import com.edayqe.learn.flink.state.CountWithOperateState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class StateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置checkPoint
        env.enableCheckpointing(60000);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMinPauseBetweenCheckpoints(30000L);
        config.setCheckpointTimeout(10000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromElements(1L, 3L, 4L, 5L, 2L, 1L, 3L, 2L, 9L, 1L)
                .flatMap(new CountWithOperateState())
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        System.out.println(value);
                        System.out.println(context.timestamp());
                    }
                });

        env.execute("test");

    }

}
