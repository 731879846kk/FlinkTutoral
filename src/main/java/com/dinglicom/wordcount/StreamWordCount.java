package com.dinglicom.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 无界流使用 dataStream 处理的整个过程

        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 实际参数应该读取配置文件 ,主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        // 2 读取文本流  监听某个文件
        DataStreamSource<String> lineDataStream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
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
