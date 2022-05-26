package com.gzl0ng.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 郭正龙
 * @date 2021-10-22
 */
public class TypeInterceptor implements Interceptor {

    //声明一个集合用于存放拦截器处理后的事件
    private List<Event> addHeaderEvent;

    @Override
    public void initialize() {
        //初始化用于存放拦截器处理后的事件
        addHeaderEvent = new ArrayList<>();
    }

    //单个事件处理
    @Override
    public Event intercept(Event event) {
        //1.获取header和body
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        //2.根据body中是否包含“atguigu”添加不同的头信息
        if (body.contains("hello")){

            headers.put("topic","first");
        }else {
            headers.put("topic","second");
        }


        //返回数据,if返回null可以做过滤
        return event;
    }

    //批量事件处理
    @Override
    public List<Event> intercept(List<Event> list) {
        //1.清空集合
        addHeaderEvent.clear();

        //2.遍历events
        for (Event event : list){
            addHeaderEvent.add(intercept(event));
        }

        //3.返回数据

        return addHeaderEvent;
    }

    @Override
    public void close() {

    }

    public static  class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
