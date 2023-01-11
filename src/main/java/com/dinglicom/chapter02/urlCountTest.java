package com.dinglicom.chapter02;


import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/*
*  每个url的点击次数 count
* */
public class urlCountTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource())
                                    // 乱序流
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                ;

        stream.print("input");

        // 计算url的个数
        stream.keyBy(data ->data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 增量聚合
                .aggregate(new urlCountAgg(),new urlCountResult())

                .print();

        env.execute();
    }

    //增量聚合，来一条加一 输入  中间状态   输出
    public static class urlCountAgg implements AggregateFunction<Event,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 包装窗口信息，输出
    // 四个参数，  输入  输出  类型  窗口类型
    public static class urlCountResult extends ProcessWindowFunction<Long,urlBean,String, TimeWindow>{

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<urlBean> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long uv = elements.iterator().next();
            out.collect(new urlBean(url,uv,start,end));
        }
    }
}
