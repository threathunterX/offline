package com.threathunter.bordercollie.slot.util;

/**
 * Created by yy on 17-11-16.
 */
public class TopSortNode {
    private String key;
    Number value;

    public TopSortNode(String key, Number value) {
        this.setKey(key);
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (this.getClass() != obj.getClass()) return false;
        TopSortNode o = (TopSortNode) obj;
        if (getKey().equals(o.getKey())) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 7;
        result = 31 * result + key.hashCode();
        return result;
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
