package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * Created by daisy on 17-11-22
 */
public abstract class PrimaryData<T> {
    private byte[] rawData;

    public abstract T getResult();

    public byte[] getRawData() {
        return rawData;
    }

    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }
}
