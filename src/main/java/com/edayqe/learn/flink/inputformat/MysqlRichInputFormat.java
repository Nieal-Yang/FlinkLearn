package com.edayqe.learn.flink.inputformat;

import com.edayqe.learn.flink.entity.DataSource;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MysqlRichInputFormat extends RichInputFormat<Row, InputSplit> {

    protected List<DataSource> sourceList;

    protected int numPartitions;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected transient Statement currentStatement;

    protected transient ResultSet currentResultSet;

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int min) throws IOException {
        MysqlInputSplit[] inputSplits = new MysqlInputSplit[numPartitions];
        int sourceSize = sourceList == null ? 0 : sourceList.size();
        for (int i = 0; i < sourceSize; i++) {
            int inplitIndex = i % numPartitions;
            List<DataSource> curSourceList = inputSplits[inplitIndex].getSourceList();
            if (curSourceList == null) curSourceList = new ArrayList<>();
            curSourceList.add(sourceList.get(i));
            inputSplits[inplitIndex].setSourceList(curSourceList);
        }
        return inputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
