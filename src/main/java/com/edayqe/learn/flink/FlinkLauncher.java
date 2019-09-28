package com.edayqe.learn.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class  FlinkLauncher {

    public static void main(String[] args) throws Exception {

        int port = 8087;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置并行度，默认为当前机器cpu核心数
        env.setParallelism(1);

        DataStream<String> textStream = env.socketTextStream("localhost", port, "\n");

        DataStream<Tuple2<String, Long>> inputMap = textStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split("#");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;

            //最大乱序时间为10s
            final Long maxOutOrder = 10000L;

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOrder);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousTimeStamp) {
                Long timeStamp = element.f1;
                currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                //long threadId = Thread.currentThread().getId();
                System.out.println("键值 :" + element.f0 + ",事件:[ " + df.format(element.f1) + " ],currentMaxTimestamp:[ " +
                        df.format(currentMaxTimestamp) + " ],水印时间:[ " + df.format(getCurrentWatermark().getTimestamp()) + " ]");
                return timeStamp;
            }
        });

        DataStream<String> windowedStream = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<>();
                        iterable.forEach(t -> arrarList.add(t.f1));
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 键值:" + key + "\n 触发窗内数据个数 : " + arrarList.size()
                                + "\n触发窗起始数据:" + sdf.format(arrarList.get(0))
                                + "\n触发窗最后（可能是延时）数据:" + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "\n实际窗起始和结束时间:" + sdf.format(timeWindow.getStart()) + "《----》" + sdf.format(timeWindow.getEnd()) + "\n";
                        collector.collect(result);
                    }
                });

        windowedStream.print();

        env.execute("eventTime-waterMark-test");


    }
}
