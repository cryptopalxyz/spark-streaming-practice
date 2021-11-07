package com.imooc.bigdata.log.utils;

import com.imooc.bigdata.gen.LogGenerator;

import java.sql.Time;

import static java.lang.Thread.sleep;

public class Test {
    public static void main(String[] args) {
        String url = "http://localhost:9527/pk-web/upload";
        String code = "B819C95D2A2A41C1";
        try {
            sleep(2000);
            LogGenerator.generator(url, code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
