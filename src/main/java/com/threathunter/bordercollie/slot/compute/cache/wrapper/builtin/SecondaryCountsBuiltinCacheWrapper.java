package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
public abstract class SecondaryCountsBuiltinCacheWrapper<T> extends BuiltinCacheWrapper<T, Integer> {

    public SecondaryCountsBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected final boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 2) {
            return true;
        }
        return false;
    }

    protected final void checkInitial(final String firstKey) {
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

    public static class SecondaryCountBuiltinCacheWrapper extends SecondaryCountsBuiltinCacheWrapper<Object> {
        public static final CacheType TYPE = CacheType.SECONDARY_COUNT;

        public SecondaryCountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public int getCacheSize() {
            return 1;
        }

        @Override
        public Integer addData(final Object value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableInt> map = (Map<String, MutableInt>) this.cacheStore.getCache(keys[0])[offset];

            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableInt(1));
                return 1;
            }
//            Object obj = map.get(keys[1]);
//            if (obj instanceof Integer) {
//                System.out.println();
//            }
            MutableInt cache = map.get(keys[1]);
            cache.increment();
            return cache.intValue();
        }

        @Override
        public final Integer getData(final String... keys) {
            try {
                Map<String, MutableInt> map = (Map<String, MutableInt>) this.cacheStore.getCache(keys[0])[offset];
                if (map == null) {
                    return 0;
                }
                MutableInt cache = map.get(keys[1]);
                if (cache == null) {
                    return 0;
                }
                return cache.intValue();
            } catch (Exception e) {
                return 0;
            }
        }
    }

    public static class SecondaryDistinctCountBuiltinCacheWrapper extends SecondaryCountsBuiltinCacheWrapper<String> {
        public static final CacheType TYPE = CacheType.SECONDARY_DISTINCT_COUNT;

        public SecondaryDistinctCountBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public int getCacheSize() {
            return 1;
        }

        @Override
        public Integer addData(final String value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, Set> map = (Map<String, Set>) this.cacheStore.getCache(keys[0])[offset];

            if (!map.containsKey(keys[1])) {
                HashSet set = new HashSet<>();
                set.add(value);
                map.put(keys[1], set);
                return set.size();
            }

            Set cache = map.get(keys[1]);
            cache.add(value);
            return cache.size();

        }

        @Override
        public final Integer getData(final String... keys) {
            Map<String, Set> map = (Map<String, Set>) this.cacheStore.getCache(keys[0])[offset];
            if (map == null) {
                return 0;
            }

            Set cache = map.get(keys[1]);
            return cache == null ? 0 : cache.size();
        }
    }

}
