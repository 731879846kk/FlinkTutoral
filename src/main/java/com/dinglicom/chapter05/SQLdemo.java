package com.dinglicom.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLdemo {

    public static void main(String[] args)  throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE flink_doris2 (\n" +
                "siteid INT, \n" +
                "citycode INT, \n" +
                "username STRING, \n" +
                "pv BIGINT \n" +
                ") \n" +
                "WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = 'hadoop102:8031',\n" +
                " 'table.identifier' = 'test_db.table2', \n " +
                " 'username' = 'root', \n" +
                " 'password' = '000000' \n"  +
                ") \n") ;


        /*
        tableEnv.executeSql("SET 'execution.checkpointing.interval' = '10s';\n" +
                "CREATE TABLE flink_doris_sink (\n" +
                "    siteid int,\n" +
                "    citycode INT,\n" +
                "    username string,\n" +
                "    pv bigint \n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'hadoop102:8031',\n" +
                "      'table.identifier' = 'test_db.table2',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000',\n" +
                "      'sink.label-prefix' = 'doris_label'\n" +
                ");");
            */

        //tableEnv.executeSql("insert into flink_doris_sink(siteid,citycode,username,pv) values(123,1,'MMMM',9)");
        tableEnv.executeSql("select * from flink_doris2").print();
    }
}
