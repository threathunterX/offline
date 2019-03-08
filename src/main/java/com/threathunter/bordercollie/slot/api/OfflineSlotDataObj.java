package com.threathunter.bordercollie.slot.api;

/**
 * Created by toyld on 2/14/17.
 */
public class OfflineSlotDataObj {
    private byte[] key;
    private byte[] value;

    public OfflineSlotDataObj(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
