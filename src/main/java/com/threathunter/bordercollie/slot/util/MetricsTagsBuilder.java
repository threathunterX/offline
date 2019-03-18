package com.threathunter.bordercollie.slot.util;

import com.threathunter.variable.DimensionType;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class MetricsTagsBuilder {
    public static Map<String, Object> buildNormalTags(boolean isSuccess, String eventName, DimensionType dimension, String shard) {
        Map<String, Object> map = new HashMap<>();
        map.put("is_success", isSuccess);
        map.put("event_name", eventName);
        map.put("dimension", dimension.toString());
        map.put("shard", shard);

        return map;
    }

    public static Map<String, Object> buildErrorTags(ERROR_TYPE errorType, String eventName, DimensionType dimension, String shard) {
        Map<String, Object> map = new HashMap<>();
        map.put("error_type", errorType.toString());
        map.put("event_name", eventName);
        map.put("dimension", dimension.toString());
        map.put("shard", shard);

        return map;
    }

    public enum ERROR_TYPE {
        expire
    }
}
