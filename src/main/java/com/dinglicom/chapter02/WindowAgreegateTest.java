package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;
import java.util.HashSet;

public class WindowAgreegateTest {

    public static void main(String[] args) throws  Exception{

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
        stream.keyBy(data -> data.user).window(
                TumblingEventTimeWindows.of(Time.seconds(20)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long,Integer>, String>() {

                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        // 和是0   个数是0
                        return Tuple2.of(0L,0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.timestamp,accumulator.f1+1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> accumulator) {
                        Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                        return timestamp.toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                    }
                }).print()
                ;
        env.execute();

    }


    public  static  class  AvgPV implements AggregateFunction<Event, Tuple2<HashSet<String>,Long>, Long >{


        // 创建累加器
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<String>(),0L);
        }


        //累加方法  属于本窗口的数据来一条累加一次，并返回累加
        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0,accumulator.f1+1);
        }

        // 窗口闭合时将数据发送到下游
        @Override
        public Long getResult(Tuple2<HashSet<String>, Long> accumulator) {
            return  accumulator.f1/accumulator.f0.size();
        }


        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }
}
