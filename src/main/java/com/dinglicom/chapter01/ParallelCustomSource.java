package com.dinglicom.chapter01;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;


public class ParallelCustomSource implements ParallelSourceFunction<Integer> {

    private boolean running  = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (running){
            ctx.collect(random.nextInt());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
