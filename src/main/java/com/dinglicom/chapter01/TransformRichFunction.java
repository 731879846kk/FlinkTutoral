package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

public class TransformRichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L)
        );

        elementStream.map(new myRichFunction()).print();

        env.execute();
    }

    public static class myRichFunction extends RichMapFunction<Event, Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 打印一条信息
            System.out.println("open 生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close 生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }
    }
}
