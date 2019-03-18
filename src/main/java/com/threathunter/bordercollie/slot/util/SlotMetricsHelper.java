package com.threathunter.bordercollie.slot.util;


import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class SlotMetricsHelper {
    private static final SlotMetricsHelper INSTANCE = new SlotMetricsHelper();

    private String workingHour;
    private String db;

    private SlotMetricsHelper() {
        this.workingHour = "";
        this.db = "nebula.online";
    }

    public static SlotMetricsHelper getInstance() {
        return INSTANCE;
    }

    public void setWorkingHour(final String workingHour) {
        this.workingHour = workingHour;
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
        tags.put("hour", this.workingHour);
        return tags;
    }
}
