package com.edayqe.learn.flink.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class SourceFromFile extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/yangxiang/Documents/test.txt"));
        while (isRunning) {
            String line = bufferedReader.readLine();
            if (StringUtils.isBlank(line)) {
                continue;
            }
            sourceContext.collect(line);
            TimeUnit.SECONDS.sleep(60);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
