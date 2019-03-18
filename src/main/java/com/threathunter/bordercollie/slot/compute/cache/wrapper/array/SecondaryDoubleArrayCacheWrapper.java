package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;


import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;

/**
 * 
 */
public abstract class SecondaryDoubleArrayCacheWrapper extends ByteArrayCacheWrapper<Double, Double> {
    protected final HashType keyHashType;
    protected final int subSize;
    protected int subStartOffset;

    public SecondaryDoubleArrayCacheWrapper(final CacheWrapperMeta meta, final int subSize) {
        super(meta);
        this.keyHashType = meta.getSecondaryKeyHashType();
        this.subSize = subSize;
    }

    @Override
    public void updateStoreInfo(final CacheStore store, int offset) {
        super.updateStoreInfo(store, offset);
        this.subStartOffset = this.offset + 4;
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
    }

    @Override
    public Double getData(final String... keys) {
        if (nullOrEmpty(keys)) {
            return null;
        }
        byte[] cache = this.cacheStore.getCache(keys[0]);
        if (cache == null) {
            return null;
        }

        int hash = HashType.getMurMurHash(keys[1]);
        int searchOffset = ByteArrayUtil.binarySearchInt32(cache, this.subStartOffset, ByteArrayUtil.getInt(this.offset, cache), subSize, hash);
        if (searchOffset > 0) {
            return ByteArrayUtil.getDouble(searchOffset + 4, cache);
        }
        return null;
    }

    @Override
    public int getCacheSize() {
        return 4 + 20 * (4 + 8);
    }

    public static class SecondarySumDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_SUM_DOUBLE;

        public SecondarySumDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);
            // can't equal 0, because at least first 4 bytes will be the count of sub keys
            if (searchOffset > 0) {
                return ByteArrayUtil.addDouble(searchOffset + 4, target, value);
            }
            if (subTotal >= 20) {
                return null;
            }

            int insertOffset = searchOffset * -1;
            ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
            ByteArrayUtil.addInt(this.offset, target, 1);
            return ByteArrayUtil.putDouble(insertOffset + 4, target, value);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class SecondaryFirstDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_FIRST_DOUBLE;

        public SecondaryFirstDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset > 0) {
                return null;
            }

            if (subTotal >= 20) {
                return null;
            }

            int insertOffset = searchOffset * -1;
            ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
            ByteArrayUtil.addInt(this.offset, target, 1);
            return ByteArrayUtil.putDouble(insertOffset + 4, target, value);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class SecondaryLastDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_LAST_DOUBLE;

        public SecondaryLastDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            int insertOffset = searchOffset;
            if (searchOffset < 0) {
                if (subTotal >= 20) {
                    return null;
                }
                insertOffset *= -1;
                ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
                ByteArrayUtil.addInt(this.offset, target, 1);
            }

            return ByteArrayUtil.putDouble(insertOffset + 4, target, value);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class SecondaryAvgDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_AVG_DOUBLE;

        public SecondaryAvgDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 16);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset > 0) {
                int count = ByteArrayUtil.addInt(searchOffset + 4, target, 1);
                double sum = ByteArrayUtil.addDouble(searchOffset + 8, target, value);
                return sum / count;
            }
            if (subTotal >= 20) {
                return null;
            }

            int insertOffset = searchOffset * -1;
            ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
            ByteArrayUtil.putInt(insertOffset + 4, target, 1);
            ByteArrayUtil.addInt(this.offset, target, 1);
            return ByteArrayUtil.putDouble(insertOffset + 8, target, value);
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null) {
                return null;
            }

            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(cache, this.subStartOffset, ByteArrayUtil.getInt(this.offset, cache), subSize, hash);
            if (searchOffset > 0) {
                int count = ByteArrayUtil.getInt(searchOffset + 4, cache);
                if (count == 0) {
                    return null;
                }
                double sum = ByteArrayUtil.getDouble(searchOffset + 8, cache);

                return sum / count;
            }
            return null;
        }

        @Override
        public int getCacheSize() {
            // 4 for count of sub-key
            // max 20 sub-keys
            // every sub-key: 4 for hash, 4 for count, 8 for sum double
            return 4 + 20 * (4 + 4 + 8);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class SecondaryMaxDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MAX_DOUBLE;

        public SecondaryMaxDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset < 0) {
                if (subTotal >= 20) {
                    return null;
                }
                int insertOffset = searchOffset * -1;
                ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
                ByteArrayUtil.addInt(this.offset, target, 1);
                return ByteArrayUtil.putDouble(insertOffset + 4, target, value);
            } else {
                double origin = ByteArrayUtil.getDouble(searchOffset + 4, target);
                if (value > origin) {
                    return ByteArrayUtil.putDouble(searchOffset + 4, target, value);
                }
                return origin;
            }
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class SecondaryMinDoubleArrayCacheWrapper extends SecondaryDoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MIN_DOUBLE;

        public SecondaryMinDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset < 0) {
                if (subTotal >= 20) {
                    return null;
                }
                int insertOffset = searchOffset * -1;
                ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
                ByteArrayUtil.addInt(this.offset, target, 1);
                return ByteArrayUtil.putDouble(insertOffset + 4, target, value);
            } else {
                double origin = ByteArrayUtil.getDouble(searchOffset + 4, target);
                if (value < origin) {
                    return ByteArrayUtil.putDouble(searchOffset + 4, target, value);
                }
                return origin;
            }
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }
}

