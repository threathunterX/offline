package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.BuiltinCacheWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 17/3/16.
 */
public class BuiltinCacheStore implements CacheStore {
    private Map<String, Object[]> cacheMap;
    private final List<CacheWrapper> wrappers;
    Logger logger = LoggerFactory.getLogger(BuiltinCacheStore.class);
    private int cacheLenPerKey;

    public BuiltinCacheStore(final List<CacheWrapper> cacheWrappers) {
        this.cacheMap = new HashMap<>();
        logger.info("store:{} cacheMap:{}", hashCode(), cacheMap.hashCode());
        this.wrappers = cacheWrappers;

        this.initial();
    }

    private void initial() {
        wrappers.forEach(wrapper -> {
            if (!(wrapper instanceof BuiltinCacheWrapper)) {
                throw new RuntimeException("wrapper should be builtin wrapper");
            }
            wrapper.updateStoreInfo(this, cacheLenPerKey);
            this.cacheLenPerKey += wrapper.getCacheSize();
        });
    }

    @Override
    public final Object[] allocate(final String key) {
        logger.info("----BuildinCacheStore instance{}. allocate:key = {}, ---cacheLenPerKey={}", hashCode(), key, cacheLenPerKey);
        Object[] cache = new Object[cacheLenPerKey];
        this.cacheMap.put(key, cache);
        return cache;
    }

    @Override
    public final Object[] getCache(final String key) {
        logger.trace("----get cache key = {}", key);
        return this.cacheMap.get(key);
    }

    @Override
    public void clearAll() {
        logger.trace("-----!!!IMPORTANT!clear all cache.");
        this.cacheMap = new HashMap<>();
    }

    public Iterator<String> getKeyIterator() {
        return this.cacheMap.keySet().iterator();
    }
}
