package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import com.dinglicom.chapter02.urlBean;
import com.dinglicom.chapter02.urlCountTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class TopNTest {

    // 统计20秒内，url热门访问次数，每隔10秒更新一次

    public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.getConfig().setAutoWatermarkInterval(100);

            SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
                    // 乱序流
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                @Override
                                public long extractTimestamp(Event element, long recordTimestamp) {
                                    return element.timestamp;
                                }
                            }));

            // 需要做的，统计  ，排序  ，根据url

            // 按照url分组，统计窗口内每个url访问量
         stream.map(data -> data.url)
                // 开窗，滑动窗口
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)))
                // 做聚合统计
                .aggregate(new urlHashMapCountAgg(), new urlAllWindowResult())
                .print();

        //urlcountStream.print("url count");

        // 对于 同一窗口统计的访问量进行排序  收集排序

        //urlcountStream.windowAll()

        env.execute();
        }

        // 实现自定义增量聚合函数
        public static class urlHashMapCountAgg implements AggregateFunction<String, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>>{

            @Override
            public HashMap<String, Long> createAccumulator() {
                return new HashMap<>();
            }

            @Override
            public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
                if(accumulator.containsKey(value)){
                    Long aLong = accumulator.get(value);
                    accumulator.put(value,aLong +1);
                }else {
                    accumulator.put(value,1L);
                }
                return accumulator;
            }

            @Override
            public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
                ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
                for (String key : accumulator.keySet()) {
                    result.add(Tuple2.of(key,accumulator.get(key)));
                }
                result.sort(new Comparator<Tuple2<String, Long>>() {
                    @Override
                    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                        return o2.f1.intValue() - o1.f1.intValue();
                    }
                });
                return result;
            }

            @Override
            public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
                return null;
            }
        }


        // 实现自定义全窗口函数   包装输出信息
        public static  class  urlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,String, TimeWindow>{

            @Override
            public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
                ArrayList<Tuple2<String, Long>> list = elements.iterator().next();

                StringBuilder result = new StringBuilder();
                result.append("--------------------------\n");
                result.append("窗口结束时间" + new Timestamp(context.window().getEnd()) + "\n");
                // 取list前两个包装信息输出
                for (int i = 0; i < 3; i++) {
                    Tuple2<String, Long> currentTuple = list.get(i);
                    String info = "No." + (i+1) + " " + "url: " + currentTuple.f0
                            + "访问量:" + currentTuple.f1 + "\n";
                    result.append(info);
                }
                result.append("--------------------------");
                out.collect(result.toString());
            }

        }
    }

