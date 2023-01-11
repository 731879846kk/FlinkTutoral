package com.dinglicom.chapter01;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 自定义数据源
public class SouceCustomTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        // 并行度为1 的source
        //DataStreamSource<Event> CustomStreamSource = env.addSource(new clickSource());
        //CustomStreamSource.print();

        // 并行度可以修改 的source
        DataStreamSource<Integer> objectDataStreamSource = env.addSource(new ParallelCustomSource()).setParallelism(2);
        objectDataStreamSource.print();
        env.execute();
    }
}
