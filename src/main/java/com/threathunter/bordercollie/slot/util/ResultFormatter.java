package com.threathunter.bordercollie.slot.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yy on 17-11-23.
 */
public class ResultFormatter {
    public static Map<String, Object> parse(long ts, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put("key", ts);
        map.put("value", value);
        return map;
    }

    public static Map<String, Object> parse(Object value) {
        return parse(System.currentTimeMillis(), value);
    }
}
