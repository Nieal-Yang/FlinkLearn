package com.edayqe.learn.flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile(params.get("input")).setParallelism(2);

        env.getConfig().setGlobalJobParameters(params);

        final int windowSize = params.getInt("window", 10);
        final int slideSize = params.getInt("slide", 5);

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(",");
                for (String str : strings) {
                    Tuple2<String, Integer> tuple = new Tuple2<>(str, 1);
                    out.collect(tuple);
                }
            }
        }).slotSharingGroup("flatMap_sg")
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1).setParallelism(3).slotSharingGroup("sum_sg");
        counts.print().setParallelism(3);

        env.execute("windowWordCoujnt");
    }
}
