package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by daisy on 17/3/16.
 */
public abstract class GlobalCountsBuiltinCacheWrapper<T> extends BuiltinCacheWrapper<T, Integer> {
    protected static final String GLOBAL_KEY = SlotUtils.totalKey;

    public GlobalCountsBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    public static class GlobalCountBuiltinCacheWrapper extends GlobalCountsBuiltinCacheWrapper<Object> {
        public static final CacheType TYPE = CacheType.GLOBAL_COUNT;

        public GlobalCountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public final Integer addData(final Object value, final String... keys) {
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (this.cacheStore.getCache(GLOBAL_KEY)[offset] == null) {
                this.cacheStore.getCache(GLOBAL_KEY)[offset] = new MutableInt(0);
            }
            MutableInt cache = (MutableInt) this.cacheStore.getCache(GLOBAL_KEY)[offset];
            cache.increment();
            return cache.intValue();
        }

        @Override
        public final Integer getData(final String... keys) {
            Object[] all = this.cacheStore.getCache(GLOBAL_KEY);
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

    public static class GlobalDistinctCountBuiltinCacheWrapper extends GlobalCountsBuiltinCacheWrapper<String> {
        public static final CacheType TYPE = CacheType.GLOBAL_DISTINCT_COUNT;

        public GlobalDistinctCountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            Object[] all = this.cacheStore.getCache(GLOBAL_KEY);
            if (all == null) {
                return 0;
            }
            return all[offset];
        }

        @Override
        public final Integer addData(final String value, final String... keys) {
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (this.cacheStore.getCache(GLOBAL_KEY)[offset] == null) {
                this.cacheStore.getCache(GLOBAL_KEY)[offset] = new HashSet<String>();
            }
            Set<String> sets = (HashSet<String>) this.cacheStore.getCache(GLOBAL_KEY)[offset];
            sets.add(value);
            return sets.size();
        }

        @Override
        public final Integer getData(final String... keys) {
            Object[] all = this.cacheStore.getCache(GLOBAL_KEY);
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
