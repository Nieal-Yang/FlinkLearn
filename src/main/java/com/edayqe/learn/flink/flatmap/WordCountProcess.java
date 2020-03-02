package com.edayqe.learn.flink.flatmap;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class WordCountProcess extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private ValueState<Tuple2<String, Integer>> valueState;
    private ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Tuple2<String, Integer>> valueStateDescriptor = new ValueStateDescriptor<>("valueStateDesc",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));
        valueState = getRuntimeContext().getState(valueStateDescriptor);

        ValueStateDescriptor<Long> timerStateDescriptor = new ValueStateDescriptor<>("timerStateDesc",
                TypeInformation.of(new TypeHint<Long>() {
                }));
        timerState = getRuntimeContext().getState(timerStateDescriptor);
    }


    @Override
    public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Tuple2<String, Integer> currentState = valueState.value();

        // 初始化ValueState值
        if (null == currentState) {
            currentState = new Tuple2<>(input.f0, 0);
            Long ttlTime = System.currentTimeMillis() + 3 * 60 * 1000;
            ctx.timerService().registerProcessingTimeTimer(ttlTime);

            // 保存定时器触发时间到状态中
            timerState.update(ttlTime);
        }

        Tuple2<String, Integer> newState = new Tuple2<>(currentState.f0, currentState.f1 + input.f1);

        // 更新ValueState值
        valueState.update(newState);

        collector.collect(newState);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        System.out.println("clear...");
        valueState.clear();
        // 清除定时器
        Long ttlTime = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(ttlTime);
    }
}