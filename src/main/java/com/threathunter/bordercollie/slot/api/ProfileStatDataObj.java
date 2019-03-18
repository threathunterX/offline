package com.threathunter.bordercollie.slot.api;

/**
 * 
 */
public class ProfileStatDataObj {
    private String key;
    private Object value;
    private String dimension;

    public ProfileStatDataObj() {

    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getDimension() {
        return dimension;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
