package com.dinglicom.chapter05;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class statTest {
    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        stream.keyBy(data ->data.user)
                .flatMap(new MyFlatMap())
                .print();

        env.execute();
    }

    // 实现自定义的 flatMapFunction   用于测试keyed stat
    public static class MyFlatMap extends RichFlatMapFunction<Event,String>{

        // 定义状态

        ValueState<Event> myValueStat ;

        ListState<Event> myListState ;

        MapState<String,Long> myMapState;

        ReducingState<Event>  myReduceState;

        AggregatingState<Event,String>  myAggregateState;



        @Override
        public void open(Configuration parameters) throws Exception {
            myValueStat =  getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-state",Event.class));
            myListState =  getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-Liststate",Event.class));
            myMapState =  getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("my-Mapstate",String.class,Long.class));

            myReduceState =  getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-Reducestate",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user,value1.url,value2.timestamp);
                        }
                    }
                    , Event.class));
            myValueStat =  getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-state",Event.class));


        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 访问和更新状态
            System.out.println(myValueStat.value());

            myValueStat.update(value);
            myListState.add(value);

            System.out.println("my value"+myValueStat.value());
        }
    }
}
