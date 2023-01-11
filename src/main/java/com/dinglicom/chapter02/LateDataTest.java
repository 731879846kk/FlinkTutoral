package com.dinglicom.chapter02;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        //SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
         SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102",7777)
                 .map(new MapFunction<String, Event>() {

                     @Override
                     public Event map(String value) throws Exception {
                         String[] s = value.split(",");
                         return  new Event(s[0].trim(),s[1].trim(),Long.valueOf(s[2].trim()));
                     }
                 }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    })
                 );

        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late"){};

        stream.print("input");

        // 计算url的个数
        SingleOutputStreamOperator<urlBean> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 增量聚合
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new urlCountTest.urlCountAgg(), new urlCountTest.urlCountResult())
                ;
        result.print("result");
        result.getSideOutput(late).print("late");
        env.execute();
    }
}
