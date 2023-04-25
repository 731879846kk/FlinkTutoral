package com.dinglicom.chapter05;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkSinkDoris {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties  prop = new Properties();
        prop.setProperty("format","json");
        prop.setProperty("strip_outer_array", "true");

        DataStreamSource<String> stream = env.fromElements(
                "{\"siteid\":\"99\",\"citycode\":\"3\",\"username\":\"MnMn\",\"pv\":\"100\"}",
                "{\"siteid\":\"99\",\"citycode\":\"3\",\"username\":\"MnMn\",\"pv\":\"123\"}"
        );

        stream.print();

        stream.addSink(DorisSink.sink(
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(3)
                        .setBatchIntervalMs(1L)
                        .setMaxRetries(3)
                        .setStreamLoadProp(prop).build(),
                DorisOptions.builder()
                    .setFenodes("192.168.10.102:8031")
                    .setTableIdentifier("test_db.nploic")
                    .setUsername("root")
                    .setPassword("000000").build()
                ));

        try {
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
