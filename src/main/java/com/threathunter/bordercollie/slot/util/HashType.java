package com.threathunter.bordercollie.slot.util;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

/**
 * 
 */
public enum HashType {
    IP,
    NORMAL;

    public static int getMurMurHash(final String key) {
        if (key == null) {
            return Hashing.murmur3_32().hashString("", Charset.defaultCharset()).asInt();
        }
        return Hashing.murmur3_32().hashString(key, Charset.defaultCharset()).asInt();
    }

    public static int getHash(final String key) {
        if (key == null) {
            return "".hashCode();
        }
        return key.hashCode();
    }
}
