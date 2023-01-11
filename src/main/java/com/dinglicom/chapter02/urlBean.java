package com.dinglicom.chapter02;


import java.sql.Timestamp;

public class urlBean {
    // flink  pojo   属性必须public   必须有空参构造法和 全参构造法
    public String url;

    public Long count;

    public Long windowStrat;

    public Long windowEnd;

    public urlBean(){
    }

    public urlBean(String url, Long count, Long windowStrat, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStrat = windowStrat;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "urlBean{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStrat=" + new Timestamp(windowStrat) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
