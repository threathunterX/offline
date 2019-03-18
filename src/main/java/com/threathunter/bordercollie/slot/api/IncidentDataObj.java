package com.threathunter.bordercollie.slot.api;

import java.util.Map;

/**
 * 
 */
public class IncidentDataObj {
    private String key;
    private Map<String, Object> value;

    public IncidentDataObj(String key, Map<String, Object> value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Object> getValue() {
        return value;
    }

}
