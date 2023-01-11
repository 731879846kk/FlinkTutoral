package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {


        //转换算子
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> elementStream = env.fromElements(new Event("boby33", "./home", 10000L),
                new Event("boby44", "./home", 10000L));
        //读取后进行转换，提取字段
        SingleOutputStreamOperator<String> result = elementStream.map(new myMapper());

        // 使用匿名类实现
        SingleOutputStreamOperator<String> map = elementStream.map(new MapFunction<Event, String>() {

            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        // 使用lambda表达式
        SingleOutputStreamOperator<String> map3 = elementStream.map(data -> data.user);


        map3.print();

        env.execute();


    }

    // 自定义mapFunction
    public static class myMapper implements MapFunction<Event,String>{

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
