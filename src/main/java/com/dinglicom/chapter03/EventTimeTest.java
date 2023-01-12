package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomerSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );


        // 事件时间触发器
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currTs = ctx.timestamp();
                        out.collect("当前数据："+ctx.getCurrentKey()+"数据到达，时间戳：" + new Timestamp(currTs) + "||" +
                                "当前watermark：" + ctx.timerService().currentWatermark());

                        // 注册一个十秒后的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("当前数据："+ctx.getCurrentKey()+"定时器触发，触发时间为" + new Timestamp(timestamp) + "|| " +
                                "当前水位线为" + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();



    }


    // 自定义source
    public static  class CustomerSource implements SourceFunction<Event>{

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("Marry","./home",1000L));

            Thread.sleep(5000L);

            ctx.collect(new Event("Jerry","./home1",11000L));

            Thread.sleep(5000L);

        }

        @Override
        public void cancel() {

        }
    }
}
