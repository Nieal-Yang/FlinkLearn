package com.edayqe.learn.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class CountWithOperateState extends RichFlatMapFunction<Long, String> implements CheckpointedFunction {

    private transient ListState<Long> checkPointCountList;
    private List<Long> listBufferElements;

    @Override
    public void flatMap(Long r, Collector<String> collector) throws Exception {
        if (r == 1L) {
            if (listBufferElements.size() > 0) {
                StringBuffer sb = new StringBuffer();
                for (Long ele : listBufferElements) {
                    sb.append(ele);
                    sb.append(",");
                }
                collector.collect(sb.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(r);
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (Long ele : listBufferElements) {
            checkPointCountList.add(ele);
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<Long>("listOfNumber",
                TypeInformation.of(new TypeHint<Long>() {
                }));
        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        if (functionInitializationContext.isRestored()) {
            for (Long ele : checkPointCountList.get()) {
                listBufferElements.add(ele);
            }
        }
    }
}
