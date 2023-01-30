package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import com.dinglicom.chapter02.urlBean;
import com.dinglicom.chapter02.urlCountTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TopNAllWindowTest {

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
                        }));

        // 需要做的，统计  ，排序  ，根据url
        SingleOutputStreamOperator<urlBean> urlStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)))
                .aggregate(new urlCountTest.urlCountAgg(), new urlCountTest.urlCountResult());
        // 按照url分组，统计窗口内每个url访问量
        urlStream.print("url count");

        urlStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(3))
                .print();

    }
    // 实现自定义  keyedProcessFunciton
    public static class TopNProcessResult extends KeyedProcessFunction<Long,urlBean,String>{

        private Integer n;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        // 定义列表状态
        private ListState<urlBean> listState;

        // 在运行环境中获取状态
        

        @Override
        public void processElement(urlBean value, Context ctx, Collector<String> out) throws Exception {
                // 每来一个数据，先扔到list列表

        }
    }
}
