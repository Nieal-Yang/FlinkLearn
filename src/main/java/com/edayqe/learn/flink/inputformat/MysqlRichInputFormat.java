package com.edayqe.learn.flink.inputformat;

import com.edayqe.learn.flink.entity.DataSource;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class MysqlRichInputFormat extends RichInputFormat<Row, InputSplit> {

    protected List<DataSource> sourceList;

    protected int numPartitions;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected transient Statement currentStatement;

    public MysqlRichInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    public List<DataSource> getSourceList() {
        return sourceList;
    }

    public void setSourceList(List<DataSource> sourceList) {
        this.sourceList = sourceList;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        MysqlInputSplit[] inputSplits = new MysqlInputSplit[numPartitions];

        return inputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
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
