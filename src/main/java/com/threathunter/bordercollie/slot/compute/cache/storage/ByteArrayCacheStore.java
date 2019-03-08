package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.ByteArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.HLLUtil;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 17/3/16.
 */
public class ByteArrayCacheStore implements CacheStore {
    private Map<String, byte[]> cacheMap;
    private Map<String, HLL> hllMap;
    private final List<CacheWrapper> wrappers;

    private int cacheLenPerKey;

    public ByteArrayCacheStore(final List<CacheWrapper> cacheWrappers) {
        this.cacheMap = new HashMap<>();
        this.hllMap = new HashMap<>();
        this.wrappers = cacheWrappers;

        this.initial();
    }

    private void initial() {
        wrappers.forEach(wrapper -> {
            if (!(wrapper instanceof ByteArrayCacheWrapper)) {
                throw new RuntimeException("wrapper should be bytes-array wrapper");
            }
            wrapper.updateStoreInfo(this, cacheLenPerKey);
            this.cacheLenPerKey += wrapper.getCacheSize();
        });
    }

    @Override
    public final byte[] allocate(final String key) {
        byte[] cache = new byte[this.cacheLenPerKey];
        this.cacheMap.put(key, cache);
        return cache;
    }

    @Override
    public final byte[] getCache(final String key) {
        return this.cacheMap.get(key);
    }

    @Override
    public void clearAll() {
        this.cacheMap = new HashMap<>();
        this.hllMap = new HashMap<>();
    }

    public HLL allocateHLL(final String key) {
        HLL hll = HLLUtil.createHLL();
        this.hllMap.put(key, hll);
        return hll;
    }

    public HLL getHLL(final String key) {
        return this.hllMap.get(key);
    }

    public Iterator<String> getKeyIterator() {
        return this.cacheMap.keySet().iterator();
    }
}
