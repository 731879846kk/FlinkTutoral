package com.dinglicom.chapter01;

public class test {

    public static void main(String[] args) {
        String s = "mikey, ./product?id=1000, 10000";
        String[] line = s.split(",");
        System.out.println(line[1].trim());
    }
}
