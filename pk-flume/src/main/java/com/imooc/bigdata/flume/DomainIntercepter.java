package com.imooc.bigdata.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DomainIntercepter implements Interceptor {

    List<Event> eventList;

    @Override
    public void initialize() {
        eventList = new ArrayList<>();

    }

    @Override //单个事件
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        if (body.contains("imooc")) {
            headers.put("type", "imooc");
        } else
            headers.put("type", "other");

        return event;
    }

    @Override //多个事件
    public List<Event> intercept(List<Event> list) {
        eventList.clear();

        for (Event event: eventList) {
           eventList.add(intercept(event));
        }
        return eventList;
    }

    @Override //资源释放
    public void close() {
        eventList = null;
        
    }
    
    
    //return 
    public static class Builder implements Interceptor.Builder {


        @Override
        public Interceptor build() {
            return new DomainIntercepter();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
