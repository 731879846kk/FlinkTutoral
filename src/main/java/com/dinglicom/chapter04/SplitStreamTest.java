package com.dinglicom.chapter04;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        // 先定义输出标签
        OutputTag<Tuple3<String, String, Long>> marryTag = new OutputTag<Tuple3<String, String, Long>>("marry"){};
        OutputTag<Tuple3<String, String, Long>> AliceTag = new OutputTag<Tuple3<String, String, Long>>("Alice"){};
        //Bob
        OutputTag<Tuple3<String, String, Long>> BobTag= new OutputTag<Tuple3<String, String, Long>>("Bob"){};
        SingleOutputStreamOperator<Event> processedSteam = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("marry")) {
                    context.output(marryTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Alice")) {
                    context.output(AliceTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(BobTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }

            }
        });

        processedSteam.print("else");
        processedSteam.getSideOutput(marryTag).print("marry");
        processedSteam.getSideOutput(AliceTag).print("Alice");
        processedSteam.getSideOutput(BobTag).print("Bob");

        env.execute();

    }
}
