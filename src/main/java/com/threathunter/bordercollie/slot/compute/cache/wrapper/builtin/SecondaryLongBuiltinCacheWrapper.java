package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

/**
 * Created by toyld on 3/21/17.
 */

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17/3/21.
 */
public abstract class SecondaryLongBuiltinCacheWrapper extends BuiltinCacheWrapper<Long, Long> {

    public SecondaryLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
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

    @Override
    public Long getData(final String... keys) {
        if (nullOrEmpty(keys)) {
            return null;
        }
        Object[] cache = this.cacheStore.getCache(keys[0]);
        if (cache == null) {
            return null;
        }
        Map<String, MutableLong> map = (Map<String, MutableLong>) cache[offset];
        if (map == null || !map.containsKey(keys[1])) {
            return null;
        }
        return map.get(keys[1]).longValue();
    }

    @Override
    public int getCacheSize() {
        return 1;
    }

    public static class SecondarySumLongBuiltinCacheWrapper extends SecondaryLongBuiltinCacheWrapper {

        public SecondarySumLongBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableLong> map = (Map<String, MutableLong>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableLong(value));
                return value;
            } else {
                MutableLong current = map.get(keys[1]);
                current.add(value);
                return current.getValue();
            }
        }
    }

    public static class SecondaryFirstLongBuiltinCacheWrapper extends SecondaryLongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_FIRST_LONG;

        public SecondaryFirstLongBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableLong> map = (Map<String, MutableLong>) this.cacheStore.getCache(keys[0])[offset];
            if (map.containsKey(keys[1])) {
                return null;
            }
            map.put(keys[1], new MutableLong(value));
            return value;
        }
    }

    public static class SecondaryLastLongBuiltinCacheWrapper extends SecondaryLongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_LAST_LONG;

        public SecondaryLastLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableLong> map = (Map<String, MutableLong>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableLong(value));
            } else {
                map.get(keys[1]).setValue(value);
            }

            return value;
        }
    }

    public static class SecondaryMaxLongBuiltinCacheWrapper extends SecondaryLongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MAX_LONG;

        public SecondaryMaxLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableLong> map = (Map<String, MutableLong>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableLong(value));
                return value;
            }
            MutableLong current = map.get(keys[1]);
            if (current.longValue() < value) {
                current.setValue(value);
                return value;
            }
            return null;
        }
    }

    public static class SecondaryMinLongBuiltinCacheWrapper extends SecondaryLongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MIN_LONG;

        public SecondaryMinLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            Map<String, MutableLong> map = (Map<String, MutableLong>) this.cacheStore.getCache(keys[0])[offset];
            if (!map.containsKey(keys[1])) {
                map.put(keys[1], new MutableLong(value));
                return value;
            }
            MutableLong current = map.get(keys[1]);
            if (current.longValue() > value) {
                current.setValue(value);
                return value;
            }
            return null;
        }
    }
}

