package com.dinglicom.chapter03;

import com.dinglicom.chapter01.Event;
import com.dinglicom.chapter01.clickSource;
import com.dinglicom.chapter02.urlBean;
import com.dinglicom.chapter02.urlCountTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class TopNTest {

    // 统计20秒内，url热门访问次数，每隔10秒更新一次

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

            // 按照url分组，统计窗口内每个url访问量
        SingleOutputStreamOperator<urlBean> urlcountStream = stream.keyBy(data -> data.url)
                // 开窗，滑动窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)))
                // 做聚合统计
                .aggregate(new urlCountTest.urlCountAgg(), new urlCountTest.urlCountResult());

        urlcountStream.print("url count");

        // 对于 同一窗口统计的访问量进行排序  收集排序

        //urlcountStream.windowAll()

        env.execute();
        }
    }

