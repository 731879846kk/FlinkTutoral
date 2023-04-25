package com.dinglicom.chapter05;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPvTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        stream.print("input:");
        // 每间隔5秒输出一次状态
        stream.keyBy(data -> data.user)
                .process(new PeriodicPVFunction())
                .print();

        env.execute();
    }

    public static class PeriodicPVFunction extends KeyedProcessFunction<String,Event,String>{

        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeTs",Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            Long count = countState.value();
            if(count == null){
                countState.update(1L);
            }else{
                countState.update(count + 1L);
            }

            // 注册定时器
            if(timerTsState.value() == null){
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 *1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "--pv:" + countState.value()  + "--TS:" + timerTsState.value());
            timerTsState.clear();
        }
    }
}
