package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class ProcessWindowTest {

    public static void main(String[] args) throws Exception {
        // 全窗口函数，等待数据来完了才去计算，效率其实蛮低的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  假设没有迟到数据，可以不设置水位线
        //env.getConfig().setAutoWatermarkInterval(200);
        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
          .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            })
          );

        stream.keyBy(data ->true)
                // 滚动窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new myWindow()).print();

        env.execute();
    }

        // uv countWindow
    public static class myWindow extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> set = new HashSet<String>();
            for (Event element : elements) {
                set.add(element.user);
            }
            //  结合窗口信息 包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口：【" + new Timestamp(start) + "~" + new Timestamp(end) + "】 访客数为"
                + set.size());
        }
    }
}
