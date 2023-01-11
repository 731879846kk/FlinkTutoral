package com.dinglicom.chapter01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws  Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 1 从文件直接读取数据      最常见
        DataStreamSource<String> stream1 = env.readTextFile("input/clcks.txt");

        // 1.2 从集合中读取数据     测试使用
        ArrayList<Integer> arr = new ArrayList<>();
        arr.add(2);
        arr.add(4);
        arr.add(5);
        DataStreamSource<Integer> numberStream = env.fromCollection(arr);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("boby","./home",10000L));
        events.add(new Event("boby2","./home2",10000L));

        DataStreamSource<Event> eventStream = env.fromCollection(events);

        // 1.3 从元素中读取数据         测试使用
        DataStreamSource<Event> elementStream = env.fromElements(new Event("boby33", "./home", 10000L),
                new Event("boby44", "./home", 10000L));


        // 4 从socket 文本流进行读取           用来测试，性能不好
        DataStreamSource<String> SocketStream = env.socketTextStream("hadoop102", 7777);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.10.102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        // 5 从 kafka流中获取
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("finkkafka", new SimpleStringSchema(), properties));


        // 2 当成有界流直接打印查看
/*        stream1.print("1");
        numberStream.print("num");
        eventStream.print("2");
        elementStream.print("ele");*/
        // SocketStream.print("socket");
        kafkaStream.print("kafka");
        //end 滚动执行
        env.execute();
    }
}
