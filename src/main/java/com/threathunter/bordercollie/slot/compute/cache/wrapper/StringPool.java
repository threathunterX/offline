package com.threathunter.bordercollie.slot.compute.cache.wrapper;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by daisy on 18-2-5
 */
public class StringPool {
    private static final StringPool INSTANCE = new StringPool();

    private final ConcurrentHashMap<Integer, String> hashMap = new ConcurrentHashMap<>();

    private StringPool() {}

    public static StringPool getInstance() {
        return INSTANCE;
    }

    public String getString(Integer hash) {
        return hashMap.get(hash);
    }

    public String putString(Integer hash, String string) {
        return hashMap.put(hash, string);
    }
}
