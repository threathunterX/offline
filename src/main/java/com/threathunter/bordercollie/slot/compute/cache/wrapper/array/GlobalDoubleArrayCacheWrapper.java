package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;

import static com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants.GLOBAL_KEY;

/**
 * 
 */
public abstract class GlobalDoubleArrayCacheWrapper extends ByteArrayCacheWrapper<Double, Double> {
    protected static final byte NULL_FLAG = 0;
    protected static final byte VALUE_FLAG = 1;

    public GlobalDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    @Override
    public Object readAll(String key) {
        return null;
    }

    public static class GlobalSumDoubleArrayCacheWrapper extends GlobalDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.GLOBAL_COUNT;

        public GlobalSumDoubleArrayCacheWrapper(CacheWrapperMeta meta) {
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
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putDouble(this.offset + 1, target, value);
            }

            return ByteArrayUtil.addDouble(this.offset + 1, target, value);
        }

        @Override
        public Double getData(final String... keys) {
            byte[] cache = this.cacheStore.getCache(GLOBAL_KEY);
            if (cache == null) {
                return null;
            }
            return ByteArrayUtil.getDouble(this.offset + 1, cache);
        }

        @Override
        public int getCacheSize() {
            return 9;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }
    }
}
