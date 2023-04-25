package com.dinglicom.chapter01;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

public class SinkToDoris  {

    public SinkFunction<String> MySinkDoris(String dbName, String tablename){
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");

        SinkFunction<String> sink = DorisSink.sink(
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(3)
                        .setBatchIntervalMs(1L)
                        .setMaxRetries(3)
                        .setStreamLoadProp(pro).build(),
                DorisOptions.builder()
                        .setFenodes("192.168.10.102:8031")
                        .setTableIdentifier(dbName+"."+tablename)
                        .setUsername("root")
                        .setPassword("000000").build()
        );
        return sink;
    }

}
