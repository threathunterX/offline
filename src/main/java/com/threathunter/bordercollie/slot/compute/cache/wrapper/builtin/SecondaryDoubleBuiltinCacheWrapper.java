package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17/3/21.
 */
public abstract class SecondaryDoubleBuiltinCacheWrapper extends BuiltinCacheWrapper<Double, Double> {

    public SecondaryDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected final boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 2) {
            return true;
        }
        return false;
    }

    protected final void checkInitialCache(final String firstKey) {
        if (this.cacheStore.getCache(firstKey) == null) {
            this.cacheStore.allocate(firstKey);
        }
        if (this.cacheStore.getCache(firstKey)[offset] == null) {
            this.cacheStore.getCache(firstKey)[offset] = new HashMap<>();
        }
    }

    @Override
    public Object readAll(final String key) {
        if (key == null) {
            return null;
        }
        Object[] cache = this.cacheStore.getCache(key);
        if (cache == null) {
            return null;
        }
        return cache[offset];
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
        Map<String, MutableDouble> map = (Map<String, MutableDouble>) cache[offset];
        if (map == null) {
            return null;
        }
        return map.get(keys[1]).doubleValue();
    }

    @Override
    public int getCacheSize() {
        return 1;
    }

    public static class SecondarySumDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_SUM_DOUBLE;

        public SecondarySumDoubleBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableDouble(value));
                return value;
            } else {
                MutableDouble current = map.get(keys[1]);
                current.add(value);
                return current.doubleValue();
            }
        }
    }

    public static class SecondaryFirstDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_FIRST_DOUBLE;

        public SecondaryFirstDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];
            if (map.containsKey(keys[1])) {
                return null;
            }
            map.put(keys[1], new MutableDouble(value));
            return value;
        }
    }

    public static class SecondaryLastDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_LAST_DOUBLE;

        public SecondaryLastDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];

            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableDouble(value));
            } else {
                map.get(keys[1]).setValue(value);
            }
            return value;
        }
    }

    public static class SecondaryAvgDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_AVG_DOUBLE;

        public SecondaryAvgDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);
            if (this.cacheStore.getCache(keys[0])[offset + 1] == null) {
                this.cacheStore.getCache(keys[0])[offset + 1] = new HashMap<>();
            }

            Map<String, MutableDouble> sumMap = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];
            Map<String, MutableInt> countMap = (Map<String, MutableInt>) this.cacheStore.getCache(keys[0])[offset + 1];
            sumMap.computeIfAbsent(keys[1], k -> new MutableDouble(0)).add(value);
            countMap.computeIfAbsent(keys[1], k -> new MutableInt(0)).increment();
            return sumMap.get(keys[1]).doubleValue() / countMap.get(keys[1]).intValue();
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

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) cache[offset];
            if (map == null) {
                return null;
            }
            return map.get(keys[1]).doubleValue() / ((Map<String, MutableInt>) cache[offset + 1]).get(keys[1]).intValue();
        }

        @Override
        public Object readAll(final String key) {
            Map<String, Double> result = new HashMap<>();

            Object[] cache = this.cacheStore.getCache(key);
            if (cache == null) {
                return null;
            }

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) cache[offset];
            if (map == null) {
                return null;
            }

            Map<String, MutableInt> count = (Map<String, MutableInt>) cache[offset + 1];
            map.forEach((k, sum) -> result.put(k, sum.doubleValue() / count.get(k).intValue()));
            return super.readAll(key);
        }

        @Override
        public int getCacheSize() {
            return 2;
        }
    }

    public static class SecondaryMaxDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MAX_DOUBLE;

        public SecondaryMaxDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableDouble(value));
                return value;
            }
            if (map.get(keys[1]).longValue() < value) {
                map.get(keys[1]).setValue(value);
                return value;
            }
            return null;
        }
    }

    public static class SecondaryMinDoubleBuiltinCacheWrapper extends SecondaryDoubleBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MIN_DOUBLE;

        public SecondaryMinDoubleBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            Map<String, MutableDouble> map = (Map<String, MutableDouble>) this.cacheStore.getCache(keys[0])[offset];

            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableDouble(value));
                return value;
            }
            MutableDouble current = map.get(keys[1]);
            if (current.longValue() > value) {
                current.setValue(value);
                return value;
            }
            return null;
        }
    }
}
