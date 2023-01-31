package com.dinglicom.chapter04;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UnionSplitTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102",7777)
                .map(data -> {
                    String[]  filed = data.split(",");
                    return new Event(filed[0].trim(),filed[1].trim(),Long.valueOf(filed[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop103",7777)
                .map(data -> {
                    String[]  filed = data.split(",");
                    return new Event(filed[0].trim(),filed[1].trim(),Long.valueOf(filed[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("stream1");
        stream2.print("stream2");

        // 合并流
        stream.union(stream2)
                .process(new ProcessFunction<Event, String>() {

                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        collector.collect("水位线" + context.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }
}
