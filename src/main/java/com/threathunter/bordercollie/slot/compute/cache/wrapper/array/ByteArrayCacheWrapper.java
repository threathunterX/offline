package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;


import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.variable.exception.NotSupportException;

/**
 * 
 */
public abstract class ByteArrayCacheWrapper<T, R> implements CacheWrapper<T, R> {
    protected int offset;
    protected ByteArrayCacheStore cacheStore;
    protected CacheWrapperMeta meta;

    public ByteArrayCacheWrapper(final CacheWrapperMeta meta) {
        this.meta = meta;
    }

    @Override
    public CacheWrapperMeta getWrapperMeta() {
        return this.meta;
    }

    @Override
    public void updateStoreInfo(final CacheStore store, int offset) {
        if (!(store instanceof ByteArrayCacheStore))
            throw new NotSupportException("the cache store is not  a byteArrayCacheWrapper.");
        if (offset < 0)
            throw new NotSupportException("the offset should be > 0");
        this.offset = offset;
        this.cacheStore = (ByteArrayCacheStore) store;
    }
}
