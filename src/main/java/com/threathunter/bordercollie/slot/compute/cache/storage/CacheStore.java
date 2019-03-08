package com.threathunter.bordercollie.slot.compute.cache.storage;

import java.util.Iterator;

/**
 * Created by daisy on 17/3/16.
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
