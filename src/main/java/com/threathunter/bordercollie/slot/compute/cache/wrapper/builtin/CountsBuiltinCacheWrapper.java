package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 */
public abstract class CountsBuiltinCacheWrapper<T> extends BuiltinCacheWrapper<T, Integer> {

    public CountsBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    public static class CountBuiltinCacheWrapper extends CountsBuiltinCacheWrapper<Object> {
        public static final CacheType TYPE = CacheType.COUNT;

        public CountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public final Integer addData(final Object value, final String... keys) {
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableInt(0);
            }
            MutableInt cache = (MutableInt) this.cacheStore.getCache(keys[0])[offset];
            cache.increment();
            return cache.intValue();
        }

        @Override
        public final Integer getData(final String... keys) {
            Object[] all = this.cacheStore.getCache(keys[0]);
            if (all == null) {
                return 0;
            }
            MutableInt cache = (MutableInt) all[offset];
            if (cache == null) {
                return 0;
            }
            return cache.intValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class DistinctCountBuiltinCacheWrapper extends CountsBuiltinCacheWrapper<String> {
        public static final CacheType TYPE = CacheType.DISTINCT_COUNT;

        public DistinctCountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            Object[] all = this.cacheStore.getCache(key);
            if (all == null) {
                return 0;
            }
            return all[offset];
        }

        @Override
        public final Integer addData(final String value, final String... keys) {
            if (value == null) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new HashSet<String>();
            }
            Set<String> sets = (HashSet<String>) this.cacheStore.getCache(keys[0])[offset];
            sets.add(value);
            return sets.size();
        }

        @Override
        public final Integer getData(final String... keys) {
            Object[] all = this.cacheStore.getCache(keys[0]);
            if (all == null) {
                return 0;
            }
            Set<String> sets = (Set<String>) all[offset];
            return sets == null ? 0 : sets.size();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

}
