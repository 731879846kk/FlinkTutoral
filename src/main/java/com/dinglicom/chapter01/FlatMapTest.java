package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby55", "./home2", 10000L)
        );

        // 自定义flatMap
        SingleOutputStreamOperator<String> out = elementStream.flatMap(new myFlatmap());

        // 传入lambda表达式
        SingleOutputStreamOperator<String> lambdaOut =
                elementStream.flatMap((Event value, Collector<String> collectOut) -> {
            collectOut.collect(value.url);
            collectOut.collect(value.user);
        }).returns(new TypeHint<String>() {});
        lambdaOut.print("2");

        env.execute();
    }

    public static class myFlatmap implements FlatMapFunction<Event,String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
        }
    }
}
