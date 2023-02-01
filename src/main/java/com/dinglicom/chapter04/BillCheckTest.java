package com.dinglicom.chapter04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


import java.time.Duration;

public class BillCheckTest {
    public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    // 支付日志
        SingleOutputStreamOperator<Tuple3<String,String,Long>> appstream1 =  env.fromElements(
            Tuple3.of("order1","app",1000L),
            Tuple3.of("order2","app",2000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                return stringStringLongTuple3.f2;
            }
        }));

    // 来自第三方平台的支付日志
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> thirdPartstream2 = env.fromElements(
                Tuple4.of("order1","app","success",3000L),
                Tuple4.of("order2","app","success",4000L),
                Tuple4.of("order3","app","success",4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String,String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String,String, Long> stringStringLongTuple4, long l) {
                        return stringStringLongTuple4.f3;
                    }
                }));


        // 检测同一支付单是否匹配    5秒等待，收不到就报警
        // 先keyby  然后再连接

    /*       appstream1.keyBy(data ->data.f0)
                .connect(thirdPartstream2.keyBy(data -> data.f0));
      */

        // 先连接  再keyby
        appstream1.connect(thirdPartstream2).keyBy(
                data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult()).print()
        ;

        env.execute();
}

    // 自定义 processfunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String, String,String, Long>,String>{

        // 定义一个状态  如果处理时发现有相同的 更新状态
        private ValueState<Tuple3<String, String, Long>>  appEvent;

        private ValueState<Tuple4<String, String, String, Long>> thirdEvent;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEvent = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("appEvent", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));

            thirdEvent = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdEvent",Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
             // 来的是appEvent   看另一条流是否来过
            if(thirdEvent.value() != null){
                collector.collect("对账成功" + stringStringLongTuple3 + "" + thirdEvent.value());

                // 清空状态
                thirdEvent.clear();
            }else {
                // 等待  更新状态
                appEvent.update(stringStringLongTuple3);

                // 注册定时器  等待另一条流的事件
                context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, Context context, Collector<String> collector) throws Exception {
            if(appEvent.value() != null){
                collector.collect("对账成功" + stringStringStringLongTuple4 + "" + appEvent.value());

                // 清空状态
                appEvent.clear();
            }else {
                // 等待  更新状态
                thirdEvent.update(stringStringStringLongTuple4);

                // 注册定时器  等待另一条流的事件
                context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3 );
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器   判断状态  如果状态不为空，贼另一条流中事件没来
            if(appEvent.value() != null){
                out.collect("对账失败" + appEvent.value() +"" +"第三方支付平台信息未到");
            }

            if(thirdEvent.value() != null){
                out.collect("对账失败" + thirdEvent.value() +"" +"app支付平台信息未到");
            }

            appEvent.clear();
            thirdEvent.clear();
        }
    }
}
