package com.edayqe.learn.flink.inputformat;

import com.edayqe.learn.flink.entity.DataSource;
import org.apache.flink.core.io.GenericInputSplit;

import java.util.List;

public class MysqlInputSplit extends GenericInputSplit {

    private List<DataSource> sourceList;

    public MysqlInputSplit(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }


    public List<DataSource> getSourceList() {
        return sourceList;
    }

    public void setSourceList(List<DataSource> sourceList) {
        this.sourceList = sourceList;
    }

}
