package com.edayqe.learn.flink.app;

import com.edayqe.learn.flink.flatmap.WordCountFlatMap;
import com.edayqe.learn.flink.flatmap.WordCountProcess;
import com.edayqe.learn.flink.source.SourceFromFile;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountStateApp {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

        // Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // StateBackend
        StateBackend stateBackend = new FsStateBackend(new Path("file:///Users/yangxiang/Documents/data"));
        env.setStateBackend(stateBackend);

        env.addSource(new SourceFromFile())
                .setParallelism(1)
                .name("demo-source")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] arr = value.split(",");
                        for (String item : arr) {
                            out.collect(new Tuple2<>(item, 1));
                        }
                    }
                })
                .name("demo-flatMap")
                .keyBy(0)
                .process(new WordCountProcess())
                .print();

        env.execute("StateWordCount");
    }
}
