package com.gzl0ng.interceptor2;

import org.mortbay.util.ajax.JSON;

/**
 * @author 郭正龙
 * @date 2022-03-07
 */
public class JSONUtils {
    public static boolean isJson(String log) {
        try {
            JSON.parse(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
