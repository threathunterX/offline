package com.threathunter.bordercollie.slot.compute.cache.storage;

import java.util.Iterator;

/**
 * 
 */
public interface CacheStore {
    /**
     * Allocate cache for a new key
     *
     * @param key
     */
    Object allocate(String key);

    Object getCache(String key);

    void clearAll();

    Iterator<String> getKeyIterator();
}
