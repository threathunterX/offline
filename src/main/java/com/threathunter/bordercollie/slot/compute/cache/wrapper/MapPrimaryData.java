package com.threathunter.bordercollie.slot.compute.cache.wrapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17-11-22
 */
public abstract class MapPrimaryData extends PrimaryData {
    private Map<String, Object> primaryData = new HashMap<>(4);

    public void addPrimaryData(String primaryName, Object value) {
        this.primaryData.put(primaryName, value);
    }

    public Object getPrimaryData(String primaryName) {
        return this.primaryData.get(primaryName);
    }
}
