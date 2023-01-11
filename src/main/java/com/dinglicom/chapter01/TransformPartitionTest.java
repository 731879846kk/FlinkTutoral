package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试过滤
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("boby33", "./home1", 10000L),
                new Event("boby44", "./home3", 10000L),
                new Event("boby44", "./hom43", 16000L),
                new Event("boby55", "./home2", 10000L),
                new Event("boby55", "./home3", 12000L),
                new Event("boby66", "./home2", 17000L),
                new Event("boby77", "./home3", 18000L),
                new Event("boby88", "./home3", 19000L)
                );


        //随机分区
        //elementStream.shuffle().print().setParallelism(4);

        // 轮询分区
        //elementStream.print().setParallelism(4);

        // 重缩放分区    按照某种分组进行轮询分区
        //elementStream.rescale().print();

        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for(int i=1;i<=8;i++){
                    // 获取运行环境的子任务编号
                    if(i%2 == getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).
                //rebalance().
               // print().
                setParallelism(4);


        // 4 广播分区，把一个数据分发给下游所有并行子任务
        //elementStream.broadcast().print().setParallelism(4);

        // 5 全局分区。把所有的分区分发给某一个子任务   此时设置这个已经没有作用了
        //elementStream.global().print().setParallelism(4);

        // 6 自定义重分区   这个数据长什么样，就往哪个分区里面放
        env.fromElements(1,2,3,4,5,6,7,8).partitionCustom(
                new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key%2;
                    }
                }, new KeySelector<Integer, Integer>() {

                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }
        ).print().setParallelism(4);

        env.execute();
    }
}
