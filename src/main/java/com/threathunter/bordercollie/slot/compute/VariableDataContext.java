package com.threathunter.bordercollie.slot.compute;

import com.threathunter.common.Identifier;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17/3/6.
 */
public class VariableDataContext {
    private final Map<String, Object> variableData;


    public VariableDataContext() {
        this.variableData = new HashMap<>();
    }

    public static String getContextKey(final Identifier variableId, final String propertyName) {
        if (variableId == null) {
            return null;
        }
        return String.format("%s@@%s@@%s", variableId.getKeys().get(0), variableId.getKeys().get(1), propertyName).intern();
    }

    public Object getFromContext(final String key) {
        return this.variableData.get(key);
    }

    public Object getFromContext(final Identifier variableId, final String propertyName) {
        return this.variableData.get(getContextKey(variableId, propertyName));
    }

    public void addContextValue(final String key, final Object value) {
        this.variableData.put(key, value);
    }

    public void addContextValue(final Identifier variableId, final String propertyName, final Object value) {
        this.variableData.put(getContextKey(variableId, propertyName), value);
    }

    public Map<String, Object> getVariableDataMap() {
        return variableData;
    }

}
