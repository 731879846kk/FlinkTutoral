package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


/*
* 无法演示
* */
public class WaterMarkTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);
        // 测试过滤
        DataStream<Event> stream = (DataStream<Event>) env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L),
                new Event("boby66", "./home2", 17000L),
                new Event("boby77", "./home3", 18000L),
                new Event("boby88", "./home3", 19000L))
                //有序流的watermark生成   单调递增的生成
/*                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    // 需要告诉flink 当前设置的时间戳按照什么来
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));*/

                //乱序流处理      需要设置周期生成水位线的时间   等待时间  设置秒数
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

            stream.keyBy(data -> data.user)
                    .window(
                    // 事件时间滑动窗口
                    //SlidingEventTimeWindows.of(Time.seconds(10))
                    // 事件时间滚动窗口
                    TumblingEventTimeWindows.of(Time.seconds(10))
                    // 会话窗口
                    //EventTimeSessionWindows.withGap(Time.hours(2)

            ).reduce(new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event() ;
                }
            });




        env.execute();
    }
}
