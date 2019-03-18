package com.threathunter.bordercollie.slot.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class SlotMetrics4M5Helper {
    private static final SlotMetrics4M5Helper INSTANCE = new SlotMetrics4M5Helper();

    private String workingMinute;
    private String db;

    private SlotMetrics4M5Helper() {
        this.workingMinute = "";
        this.db = "nebula.online";
    }

    public static SlotMetrics4M5Helper getInstance() {
        return INSTANCE;
    }

    public void setWorkingMinute(final String workingMinute) {
        this.workingMinute = workingMinute;
    }

    public void setDb(final String db) {
        this.db = db;
    }

    public void addMetrics(String metricsName, Double value, String... tagKeyValues) {
        MetricsHelper.getInstance().addMetrics(this.db, metricsName, this.getBasicMap(tagKeyValues), value);
    }

    private Map<String, Object> getBasicMap(final String... keyValues) {
        Map<String, Object> tags = new HashMap<>();
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            tags.put(keyValues[i], keyValues[i + 1]);
        }
        tags.put("minute", this.workingMinute);
        return tags;
    }
}
