package com.threathunter.bordercollie.slot.compute.cache.wrapper;


import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;

/**
 * 
 */
public interface CacheWrapper<T, R> {
    /**
     * Perform compute according to different types of cache.
     * No check null for value, and keys.
     *
     * @param value current value to compute.
     * @param keys  group keys' value.
     * @return result of compute.
     */
    R addData(T value, String... keys);

    /**
     * Get the value of a variable
     *
     * @param keys group keys' value
     * @return computed value of these keys
     */
    Object getData(String... keys);

    CacheWrapperMeta getWrapperMeta();

    int getCacheSize();

    void updateStoreInfo(CacheStore store, int offset);

    PrimaryData merge(PrimaryData lastMerged, String... keys);

    Object readAll(String key);
}
