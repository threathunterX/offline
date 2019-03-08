package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableDouble;

/**
 * Created by daisy on 17/5/14.
 */
public abstract class GlobalDoubleBuiltinCacheWrapper extends BuiltinCacheWrapper<Double, Double> {
    protected static final String GLOBAL_KEY = "__GLOBAL__";

    public GlobalDoubleBuiltinCacheWrapper(CacheWrapperMeta meta) {
        super(meta);
    }

    public static class GlobalSumDoubleBuiltinCacheWrapper extends GlobalDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.GLOBAL_SUM_DOUBLE;

        public GlobalSumDoubleBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null) {
                return null;
            }
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (this.cacheStore.getCache(GLOBAL_KEY)[offset] == null) {
                this.cacheStore.getCache(GLOBAL_KEY)[offset] = new MutableDouble(0);
            }
            MutableDouble cache = (MutableDouble) this.cacheStore.getCache(GLOBAL_KEY)[offset];
            cache.add(value);
            return cache.doubleValue();
        }

        @Override
        public Double getData(String... keys) {
            Object[] all = this.cacheStore.getCache(GLOBAL_KEY);
            if (all == null) {
                return 0.0;
            }
            MutableDouble cache = (MutableDouble) all[offset];
            if (cache == null) {
                return 0.0;
            }
            return cache.doubleValue();
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
