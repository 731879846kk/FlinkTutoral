package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.process(new ProcessFunction<Event, Object>() {

            @Override
            public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
                    //{"marry","Alice","Bob","biboer"};
                    if(value.user.equals("marry")){
                        out.collect(value.user+ "clicks" + value.user);
                    }else {
                        out.collect(value.url);
                    }

                    // 还可以用ctx获取当前的timestamp
                System.out.println("当前时间戳为："+ctx.timestamp());

                    //  查看当前水位线
                System.out.println("当前时间水位线" + ctx.timerService().currentWatermark());

                // 打印任务号
                System.out.println(getRuntimeContext().getIndexOfThisSubtask());

                // 打印状态
                //System.out.println(getRuntimeContext().getState());

            }
        }).print();

        env.execute();
    }

}
