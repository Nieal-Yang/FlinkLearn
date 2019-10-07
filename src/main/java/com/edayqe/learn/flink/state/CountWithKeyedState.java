package com.edayqe.learn.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class CountWithKeyedState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;


    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        Tuple2<Long, Long> currentSum = sum.value();

        //update count
        currentSum.f0 += 1;

        //update sum
        currentSum.f1 += input.f1;

        sum.update(currentSum);

        if(currentSum.f0>=3){
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }


    }


    @Override
    public void open(Configuration configuration){
        ValueStateDescriptor<Tuple2<Long,Long>> valueStateDescriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })
                );

        sum = getRuntimeContext().getState(valueStateDescriptor);


    }
}
