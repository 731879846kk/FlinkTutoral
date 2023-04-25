package com.dinglicom.chapter05;

import com.alibaba.fastjson2.JSON;
import com.dinglicom.chapter01.DorisPojo;
import com.dinglicom.chapter01.SinkToDoris;
import com.dinglicom.chapter01.dorisSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class DorisSourceToDorisSink {

    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<DorisPojo> stream = env.addSource(new dorisSource());
        stream.print();

        SinkToDoris dorisSink = new SinkToDoris();
        SinkFunction<String> sink = dorisSink.MySinkDoris("test_db", "nploic");


        stream.keyBy(data -> data.siteid)
                .process(new KeyedProcessFunction<Integer, DorisPojo, String>() {
                    @Override
                    public void processElement(DorisPojo value, Context ctx, Collector<String> out) throws Exception {
                        //  根据keyby 把  拼接上username
                        String name = value.username + value.citycode +"";
                        value.setUsername(name);
                        out.collect(JSON.toJSONString(value));
                    }
                }).addSink(sink);


        env.execute();
    }
}
