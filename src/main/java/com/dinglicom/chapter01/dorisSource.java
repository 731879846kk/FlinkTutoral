package com.dinglicom.chapter01;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class dorisSource implements SourceFunction<DorisPojo> {

    //声明一个标志位
    private  boolean running = true;


    @Override
    public void run(SourceContext<DorisPojo> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"marry","Alice","Bob","biboer"};
        // 循环生成
        while (running){
            int siteid = random.nextInt(5) + 1;
            int citycode = random.nextInt(2) + 1;
            String name = users[random.nextInt(users.length)];
            int pv  = random.nextInt(99);
            ctx.collect(new DorisPojo(siteid,citycode,name,pv));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
            running = false;
    }
}
