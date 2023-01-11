package com.dinglicom.chapter01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

// 简单sourceFunction 并行度只能设置为1 吞吐量会比较小
public class clickSource implements SourceFunction<Event> {

    //声明一个标志位
    private  boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        //随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"marry","Alice","Bob","biboer"};
        String[] urls = {"./home","./cart","./product?id=1000"};

        // 循环生成
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timeStamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timeStamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
