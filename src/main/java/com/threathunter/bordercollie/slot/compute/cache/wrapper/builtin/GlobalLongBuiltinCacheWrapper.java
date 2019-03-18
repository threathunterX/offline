package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Set;

/**
 * 
 */
public abstract class GlobalLongBuiltinCacheWrapper extends BuiltinCacheWrapper<Long, Long> {
    protected static final String GLOBAL_KEY = SlotUtils.totalKey;

    public GlobalLongBuiltinCacheWrapper(CacheWrapperMeta meta) {
        super(meta);
    }

    public static class GlobalSumLongBuiltinCacheWrapper extends GlobalLongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.GLOBAL_SUM_LONG;

        public GlobalSumLongBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null) {
                return null;
            }
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (this.cacheStore.getCache(GLOBAL_KEY)[offset] == null) {
                this.cacheStore.getCache(GLOBAL_KEY)[offset] = new MutableLong(0);
            }
            MutableLong cache = (MutableLong) this.cacheStore.getCache(GLOBAL_KEY)[offset];
            cache.add(value);
            return cache.longValue();
        }

        @Override
        public Long getData(String... keys) {
            Object[] all = this.cacheStore.getCache(GLOBAL_KEY);
            if (all == null) {
                return 0l;
            }
            Object cache = all[offset];
            if (cache == null) {
                return 0l;
            }
            if (cache instanceof Set) {
                return (long) ((Set) cache).size();
            } else if (cache instanceof MutableInt) {
                return (long) ((MutableInt) cache).getValue();
            } else if (cache instanceof MutableLong) {
                return ((MutableLong) cache).longValue();
            } else {
                System.out.println("Warning: unknown Global Long Builtin Cache Type at getData." + cache.getClass());
                return 0l;
            }

        }

        @Override
        public int getCacheSize() {
            return 1;
        }

        @Override
        public Object readAll(final String key) {
            return this.cacheStore.getCache(GLOBAL_KEY)[offset];
        }
    }
}
