package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * 
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
