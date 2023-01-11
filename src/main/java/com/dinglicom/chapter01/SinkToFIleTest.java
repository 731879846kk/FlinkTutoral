package com.dinglicom.chapter01;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFIleTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L),
                new Event("boby66", "./home2", 17000L),
                new Event("boby77", "./home3", 18000L),
                new Event("boby88", "./home3", 19000L)
        );
        // 设置水位线
        DataStream<Event> eventDataStreamSource = env.addSource(
                new clickSource()).assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return null;
            }

            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return WatermarkStrategy.super.createTimestampAssigner(context);
            }
        });

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                new Path("./output"), new SimpleStringEncoder<>("UTF8"))
                //设置滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5))
                                .build()
                ).build();
        // 写入到文件
        elementStream.map(data->data.toString()).addSink(fileSink);

        env.execute();
    }
}
