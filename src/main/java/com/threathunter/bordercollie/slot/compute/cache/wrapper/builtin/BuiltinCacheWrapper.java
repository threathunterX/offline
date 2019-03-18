package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.storage.BuiltinCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;

/**
 * 
 */
public abstract class BuiltinCacheWrapper<T, R> implements CacheWrapper<T, R> {
    protected int offset;
    protected BuiltinCacheStore cacheStore;
    protected CacheWrapperMeta meta;

    public BuiltinCacheWrapper(final CacheWrapperMeta meta) {
        this.meta = meta;
    }

    @Override
    public CacheWrapperMeta getWrapperMeta() {
        return this.meta;
    }

    @Override
    public void updateStoreInfo(final CacheStore store, int offset) {
        this.offset = offset;
        this.cacheStore = (BuiltinCacheStore) store;
    }

    @Override
    public PrimaryData merge(PrimaryData lastMerged, String... keys) {
        return null;
    }
}

