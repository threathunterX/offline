package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Created by toyld on 3/21/17.
 */
public abstract class LongBuiltinCacheWrapper extends BuiltinCacheWrapper<Long, Long> {

    public LongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 1) {
            return true;
        }
        return false;
    }

    // TODO test
    public static class SumLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.SUM_LONG;

        public SumLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(0);
            }
            MutableLong result = (MutableLong) this.cacheStore.getCache(keys[0])[offset];
            result.add(value);

            return result.getValue();
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).getValue();
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

    public static class FirstLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.FIRST_LONG;

        public FirstLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(value);
                return value;
            }
            return null;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).longValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class LastLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.LAST_LONG;

        public LastLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
                return value;
            }
            MutableLong current = (MutableLong) this.cacheStore.getCache(keys[0])[offset];
            if (current == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(value);
                return value;
            }

            current.setValue(value);
            return value;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).longValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class MaxLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.MAX_LONG;

        public MaxLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(value);
                return value;
            }

            MutableLong current = (MutableLong) this.cacheStore.getCache(keys[0])[offset];
            if (current.longValue() < value) {
                current.setValue(value);
                return value;
            }
            return null;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).longValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class MinLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.MIN_LONG;

        public MinLongBuiltinCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Object readAll(final String key) {
            return this.getData(key);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(value);
                return value;
            }
            MutableLong current = (MutableLong) this.cacheStore.getCache(keys[0])[offset];
            if (current.longValue() > value) {
                current.setValue(value);
                return value;
            }
            return null;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).longValue();
        }

        @Override
        public int getCacheSize() {
            return 1;
        }
    }

    public static class AvgLongBuiltinCacheWrapper extends LongBuiltinCacheWrapper {
        public static final CacheType TYPE = CacheType.AVG_LONG;

        public AvgLongBuiltinCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            if (this.cacheStore.getCache(keys[0])[offset] == null) {
                this.cacheStore.getCache(keys[0])[offset] = new MutableLong(value);
                this.cacheStore.getCache(keys[0])[offset + 1] = new MutableInt(1);
            } else {
                ((MutableLong) this.cacheStore.getCache(keys[0])[offset]).add(value);
                ((MutableInt) this.cacheStore.getCache(keys[0])[offset + 1]).add(1);
            }
            return ((MutableLong) this.cacheStore.getCache(keys[0])[offset]).longValue() / ((MutableInt) this.cacheStore.getCache(keys[0])[offset + 1]).intValue();
        }

        @Override
        public Long getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Object[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || cache[offset] == null) {
                return null;
            }

            return ((MutableLong) cache[offset]).longValue() / ((MutableInt) cache[offset + 1]).intValue();
        }

        @Override
        public int getCacheSize() {
            return 2;
        }

        @Override
        public Object readAll(String key) {
            return this.getData(key);
        }
    }
}
