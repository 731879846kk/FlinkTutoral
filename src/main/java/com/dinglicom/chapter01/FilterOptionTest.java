package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterOptionTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby55", "./home2", 10000L)
        );
        // 内部类
        //SingleOutputStreamOperator<Event> filter = elementStream.filter(new myFilter());
        // 匿名类
        SingleOutputStreamOperator<Event> filter1 = elementStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "./home1".equals(value.url);
            }
        });
        
        // lambda 
        SingleOutputStreamOperator<Event> lambda = elementStream.filter(data -> "./home1".equals(data.url));

        lambda.print();
        env.execute();

    }


    public static class myFilter implements FilterFunction<Event>{


        @Override
        public boolean filter(Event value) throws Exception {
            return "./home1".equals(value.url);
        }
    }
}
