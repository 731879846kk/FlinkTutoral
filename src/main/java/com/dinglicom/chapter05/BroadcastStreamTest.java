package com.dinglicom.chapter05;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStreamTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Action> actionstream = env.fromElements(new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy"),new Action("Bob", "buy1"));


        DataStreamSource<Pattern> patternStream = env.fromElements(new Pattern("login", "pay"),
                new Pattern("login", "buy"));

        MapStateDescriptor<Void, Pattern> patterns =
                new MapStateDescriptor<>("Patterns", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(patterns);

        DataStream<Tuple2<String, Pattern>> matches = actionstream.keyBy(data -> data.userId)
                .connect(bcPatterns).process(new PatternEvaluator());

        matches.print();
        env.execute();
    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String,Action,Pattern, Tuple2<String, Pattern>>{

        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState  = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING));

        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out)
                throws Exception {
            Pattern pattern = ctx.getBroadcastState(
            new MapStateDescriptor<>("patterns", Types.VOID,
                    Types.POJO(Pattern.class))).get(null);
            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) &&
                        pattern.action2.equals(value.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(value.action);
        }



        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {


            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID,
                            Types.POJO(Pattern.class)));
            // 将广播状态更新为当前的 pattern
            bcState.put(null, value);
        }
    }

    public static class Action {
        public String userId;
        public String action;
        public Action() {
        }
        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }
        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }
    // 定义行为模式 POJO 类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;
        public Pattern() {
        }
        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }
        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}


