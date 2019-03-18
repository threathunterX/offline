package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * 
 */
public abstract class SimplePrimaryData<T> extends PrimaryData {
    private T data;

    public void setData(T data) {
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
