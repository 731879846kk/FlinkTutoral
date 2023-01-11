package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L)
        );

        // 统计用户量最活跃，访问量最大
        //1 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = elementStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                // 把两个规约的数据的 f1 值进行相加
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        // 根据当前的个数  选取当前最活跃的用户,此时不能按照user进行keyby了
        SingleOutputStreamOperator<Tuple2<String, Long>> result = reduce.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        });

        result.print();

        env.execute();


    }
}
