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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest2023 {
    public static void main(String[] args)  throws Exception{
        //获取当前运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        // 设置水位线   毫秒值
        env.getConfig().setAutoWatermarkInterval(100);
        // 读取数据源
        DataStream<Event> elementStream = (DataStream<Event>) env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L),
                new Event("boby66", "./home2", 17000L),
                new Event("boby77", "./home3", 18000L),
                new Event("boby88", "./home3", 19000L))
                // 乱序流处理
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
        }).keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))  // 滚动时间窗口
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1 + value2.f1);
                    }
                }).print()
        ;
        env.execute();

    }
}
