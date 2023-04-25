package com.dinglicom.chapter05;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DorisStreamTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("fenodes","hadoop102:8031");
        properties.put("username","root");
        properties.put("password","000000");
        properties.put("table.identifier","test_db.table2");


        env.addSource(new DorisSourceFunction(new DorisStreamOptions(properties),new SimpleListDeserializationSchema()))
        .print();

        env.execute();
    }
}
