package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import com.dinglicom.chapter02.urlBean;
import com.dinglicom.chapter02.urlCountTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

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

 /*       urlStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(3))
                .print();*/

         urlStream.keyBy(data -> data.windowEnd)
                 .process(new TopNProcessResult(2))
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
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<urlBean>("url-count-list", Types.POJO(urlBean.class))
            );
        }

        @Override
        public void processElement(urlBean value, Context ctx, Collector<String> out) throws Exception {
                // 每来一个数据，先扔到list列表
            listState.add(value);
            // 注册windowend定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.getCurrentKey() +1);


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<urlBean> urlcountList = new ArrayList<>();

            for (urlBean urlBean : listState.get()) {
                urlcountList.add(urlBean);
            }

            urlcountList.sort(new Comparator<urlBean>() {
                @Override
                public int compare(urlBean o1, urlBean o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            //包装信息

            StringBuilder result = new StringBuilder();
            result.append("--------------------------\n");
            result.append("窗口结束时间" + new Timestamp(ctx.getCurrentKey()) + "\n");
            // 取list前两个包装信息输出
            for (int i = 0; i < 2; i++) {
                urlBean currentTuple = urlcountList.get(i);
                String info = "No." + (i+1) + " " + "url: " + currentTuple.url
                        + "访问量:" + currentTuple.count + "\n";
                result.append(info);
            }
            result.append("--------------------------");
            out.collect(result.toString());
        }
    }
}
