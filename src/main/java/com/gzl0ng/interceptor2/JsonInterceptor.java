package com.gzl0ng.interceptor2;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author 郭正龙
 * @date 2022-03-06
 * com.gzl0ng.interceptor2.JsonInterceptor
 */
public class JsonInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        if (JSONUtils.isJson(log)){
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();

        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(next) == null){
                iterator.remove();
            }
        }
        return list;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new JsonInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


    @Override
    public void close() {

    }
}
