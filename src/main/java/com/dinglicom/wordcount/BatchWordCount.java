package com.dinglicom.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 批处理 ，有界的流
        // 1 创建执行环境 生成对象 Ctrl Alt  v
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = environment.readTextFile("input/word.txt");
        // 2 从文件中读取数据
        DataSource<String> lineSource = environment.readTextFile("input/word.txt");
        // 3 将每行数据进行分词 转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOnrTuple = lineSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行分词
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 按照当前word进行分组
        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = wordAndOnrTuple.groupBy(0);
        // 统计  分组内进行聚合
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);


        // 6 结果测查询输出
        sum.print();

    }
}
