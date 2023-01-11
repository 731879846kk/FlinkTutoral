package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class CombineAggProcessTest {

    // 将aggregate聚合窗口 和 全窗口 process 结合使用

    public static void main(String[] args)  throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
               .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                       .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                           @Override
                           public long extractTimestamp(Event element, long recordTimestamp) {
                               return element.timestamp;
                           }
                       })
                );


        stream.print("input");

        // data -> true 表示全量数据
        stream.keyBy(data -> true)
                // 使用aggregate  和 process 两个窗口函数
                // 滚动时间串口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 先使用aggregate 函数
                .aggregate(new myAggregate(),new uvCountResult())
                .print();

        env.execute();

    }


    // 自定义 aggregate 实现增量聚合
    public static  class myAggregate implements AggregateFunction<Event, HashSet<String>,Long>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }


    // 自定义 processWindowFunction  包装窗口输出信息

    public static  class uvCountResult extends ProcessWindowFunction<Long,String,Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long uv = elements.iterator().next();
            out.collect("窗口：【" + new Timestamp(start) + "~" + new Timestamp(end) + "】 访客数为"
                    + uv);
        }
    }
}
