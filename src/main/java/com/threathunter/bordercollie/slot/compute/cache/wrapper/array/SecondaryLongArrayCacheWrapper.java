package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17/3/21.
 */
public abstract class SecondaryLongArrayCacheWrapper extends ByteArrayCacheWrapper<Long, Long> {
    protected final HashType keyHashType;
    protected final int subSize;
    protected int subStartOffset;

    public SecondaryLongArrayCacheWrapper(final CacheWrapperMeta meta, int subSize) {
        super(meta);
        this.keyHashType = meta.getSecondaryKeyHashType();
        this.subSize = subSize;
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
    public void updateStoreInfo(final CacheStore store, int offset) {
        super.updateStoreInfo(store, offset);
        this.subStartOffset = this.offset + 4;
    }

    @Override
    public int getCacheSize() {
        return 4 + 20 * 12;
    }

    @Override
    public Long getData(final String... keys) {
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
            return ByteArrayUtil.getLong(searchOffset + 4, cache);
        }
        return null;
    }

    public static class SecondaryFirstLongArrayCacheWrapper extends SecondaryLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_FIRST_LONG;

        public SecondaryFirstLongArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            // total of sub-keys
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            // hash of secondary key
            int hash = HashType.getMurMurHash(keys[1]);
            // find the offset of the key that should be inserted to
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset > 0 || subTotal >= 20) {
                return null;
            }

            int insertOffset = searchOffset * -1;
            ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
            ByteArrayUtil.addInt(this.offset, target, 1);
            return ByteArrayUtil.putLong(insertOffset + 4, target, value);
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

    public static class SecondarySumLongArrayCacheWrapper extends SecondaryLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_SUM_LONG;

        public SecondarySumLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitialCache(keys[0]);

            byte[] target = this.cacheStore.getCache(keys[0]);
            // total of sub-keys
            int subTotal = ByteArrayUtil.getInt(this.offset, target);
            // hash of secondary key
            int hash = HashType.getMurMurHash(keys[1]);
            // find the offset of the key that should be inserted to
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subTotal, this.subSize, hash);

            if (searchOffset > 0) {
                return ByteArrayUtil.addLong(searchOffset + 4, target, value);
            }
            if (subTotal >= 20) {
                return null;
            }

            int insertOffset = searchOffset * -1;
            ByteArrayUtil.insertInt32(target, this.subStartOffset, insertOffset, subTotal, this.subSize, hash);
            ByteArrayUtil.addInt(this.offset, target, 1);
            return ByteArrayUtil.putLong(insertOffset + 4, target, value);
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

    public static class SecondaryLastLongArrayCacheWrapper extends SecondaryLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_LAST_LONG;

        public SecondaryLastLongArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
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

            return ByteArrayUtil.putLong(insertOffset + 4, target, value);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(final String key) {
            Map<Integer, Long> result = new HashMap<>();
            byte[] cache = this.cacheStore.getCache(key);
            if (cache == null) {
                return result;
            }
            int subTotal = ByteArrayUtil.getInt(this.offset, cache);
            int start = this.subStartOffset;
            while (subTotal > 0) {
                result.put(ByteArrayUtil.getInt(start, cache), ByteArrayUtil.getLong(start + 4, cache));
                start += this.subSize;
                subTotal--;
            }
            return result;
        }
    }

    public static class SecondaryMaxLongArrayCacheWrapper extends SecondaryLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MAX_LONG;

        public SecondaryMaxLongArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
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
                return ByteArrayUtil.putLong(insertOffset + 4, target, value);
            } else {
                long origin = ByteArrayUtil.getLong(searchOffset + 4, target);
                if (value > origin) {
                    return ByteArrayUtil.putLong(searchOffset + 4, target, value);
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

    public static class SecondaryMinLongArrayCacheWrapper extends SecondaryLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SECONDARY_MIN_LONG;

        public SecondaryMinLongArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta, 12);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
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
                return ByteArrayUtil.putLong(insertOffset + 4, target, value);
            } else {
                long origin = ByteArrayUtil.getLong(searchOffset + 4, target);
                if (value < origin) {
                    return ByteArrayUtil.putLong(searchOffset + 4, target, value);
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

