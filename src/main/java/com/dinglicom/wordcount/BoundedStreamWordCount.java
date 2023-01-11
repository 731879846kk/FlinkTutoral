package com.dinglicom.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {


    public static void main(String[] args) throws Exception {
        // 流处理的word count
        // 1 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 读取文件
        DataStreamSource<String> lineDateStreamSource = env.readTextFile("input/word.txt");
        // 3 转换算法
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDateStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })


                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 分组操作
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream= wordOneTuple.keyBy(data -> data.f0);

        // 5 聚合操作
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6 打印
        sum.print();

        // 7 滚动执行
        env.execute();
    }
}
