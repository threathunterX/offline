package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * 
 */
public abstract class DoubleBuiltinCacheWrapper extends BuiltinCacheWrapper<Double, Double> {

    public DoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 1) {
            return true;
        }
        return false;
    }

    // TODO test
    public static class SumDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SUM_DOUBLE;

        public SumDoubleBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableDouble(0);
            }
            MutableDouble result = (MutableDouble) this.cacheStore.getCache(keys[0])[offset];
            result.add(value);

            return result.getValue();
        }

        @Override
        public Double getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableDouble) cache[offset]).getValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }

        @Override
        public Object readAll(final String key) {
            return this.cacheStore.getCache(key)[offset];
        }
    }

    public static class FirstDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.FIRST_DOUBLE;

        public FirstDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableDouble(value);
                return value;
            }
            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableDouble) cache[offset]).getValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class LastDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.LAST_DOUBLE;

        public LastDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            this.cacheStore.getCache(keys[0])[offset] = new MutableDouble(value);
            return value;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableDouble) cache[offset]).doubleValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class AvgDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.AVG_DOUBLE;

        public AvgDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableDouble(value);
                this.cacheStore.getCache(keys[0])[offset + 1] = new MutableInt(1);
            } else {
                ((MutableDouble) this.cacheStore.getCache(keys[0])[offset]).add(value);
                ((MutableInt) this.cacheStore.getCache(keys[0])[offset + 1]).add(1);
            }
            return ((MutableDouble) this.cacheStore.getCache(keys[0])[offset]).doubleValue() / ((MutableInt) this.cacheStore.getCache(keys[0])[offset + 1]).intValue();
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableDouble) cache[offset]).doubleValue() / ((MutableInt) cache[offset + 1]).intValue();
        }

        @Override
        public int getCacheSize() {
            return 2;
        }
    }

    public static class MaxDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.MAX_DOUBLE;

        public MaxDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableDouble(value);
                return value;
            }

            if (((MutableDouble) this.cacheStore.getCache(keys[0])[offset]).doubleValue() < value) {
                ((MutableDouble) this.cacheStore.getCache(keys[0])[offset]).setValue(value);
                return value;
            }
            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableDouble) cache[offset]).doubleValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class MinDoubleBuiltinCacheWrapper extends DoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.MIN_DOUBLE;

        public MinDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            if (this.cacheStore.getCache(keys[0])[offset] == null || (Double) this.cacheStore.getCache(keys[0])[offset] > value) {
                this.cacheStore.getCache(keys[0])[offset] = value;
                return value;
            }
            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null) {
                return null;
            }

            return (Double) cache[offset];
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

}
