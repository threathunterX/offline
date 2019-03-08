package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * Created by daisy on 17-11-22
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
