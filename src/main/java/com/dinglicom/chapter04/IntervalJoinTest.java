package com.dinglicom.chapter04;

import com.dinglicom.chapter01.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {

    public static void main(String[] args) throws Exception {


        // 代码运行不出来，需要重新检查
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String,Long>> orderStream = env.fromElements(
                Tuple2.of("Mary",5000L),
                Tuple2.of("Alice",5000L),
                Tuple2.of("Bob",20000L),
                Tuple2.of("Alice",20000L),
                Tuple2.of("cary",51000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                        return stringLongTuple2.f1;
                    }
                }))
                ;

        SingleOutputStreamOperator<Event> eventStream2 = env.fromElements(
                new Event("Bob", "./home1", 2000L),
                new Event("Alice", "./home3", 3000L),
                new Event("Alice", "./hom43", 3500L),
                new Event("Bob", "./home2", 2500L),
                new Event("Alice", "./home3", 36000L),
                new Event("Bob", "./home2", 30000L),
                new Event("Bob", "./home3", 23000L),
                new Event("Bob", "./home3", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                           @Override
                                           public long extractTimestamp(Event event, long l) {
                                               return event.timestamp;
                                           }
                                       }
                ));

        orderStream.keyBy(data ->data.f0)
                .intervalJoin(eventStream2.keyBy(data -> data.user))
                .between(Time.seconds(-5),Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long> , Event, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event event, Context context, Collector<Object> collector) throws Exception {
                        collector.collect(event + "= > " + left);
                    }
                }).print();

        env.execute();
    }
}
