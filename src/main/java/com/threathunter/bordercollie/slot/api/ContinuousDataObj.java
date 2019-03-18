package com.threathunter.bordercollie.slot.api;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

/**
 * 
 */
public class ContinuousDataObj {
    private Key key;
    private Bin value;

    public ContinuousDataObj(Key key, Bin value) {
        this.key = key;
        this.value = value;
    }

    public Key getKey() {
        return key;
    }

    public Bin getValue() {
        return value;
    }
}
