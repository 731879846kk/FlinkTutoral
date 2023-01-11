package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import redis.clients.jedis.Tuple;

import java.time.Duration;

public class WindowTest {public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.getConfig().setAutoWatermarkInterval(100);
    // 测试过滤
    DataStream<Event> elementStream = (DataStream<Event>) env.fromElements(
            new Event("boby33", "./home1", 10000L),
            new Event("boby44", "./home3", 10000L),
            new Event("boby44", "./hom43", 26000L),
            new Event("boby55", "./home2", 10000L),
            new Event("boby55", "./home3", 12000L),
            new Event("boby66", "./home2", 17000L),
            new Event("boby77", "./home3", 18000L),
            new Event("boby44", "./hom43", 28000L),
            new Event("boby44", "./hom43", 29000L),
            new Event("boby88", "./home3", 19000L))        //乱序流处理
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    }));

    elementStream.map(new MapFunction<Event, Tuple2<String,Long>>() {

        @Override
        public Tuple2<String, Long> map(Event value) throws Exception {
            return Tuple2.of(value.user,1L);
        }
    })
            .keyBy(data ->data.f0)
            // 窗口分配器
            //.window(EventTimeSessionWindows.withGap(Time.seconds(2)))         时间会话窗口
            //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.minutes(5)))  // 滑动窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(1000)))       //  滚动事件窗口
            //.countWindow(10,2)                滑动计数窗口

            .reduce(new ReduceFunction<Tuple2<String,Long>>() {

                @Override
                public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                    return Tuple2.of(value1.f0,value1.f1+value2.f1);
                }
                }).print()
                ;

    env.execute();
}
}
