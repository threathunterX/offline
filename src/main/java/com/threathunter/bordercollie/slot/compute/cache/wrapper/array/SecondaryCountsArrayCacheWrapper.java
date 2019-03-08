package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;
import net.agkn.hll.HLL;

/**
 * Created by toyld on 3/22/17.
 */
public abstract class SecondaryCountsArrayCacheWrapper<T> extends ByteArrayCacheWrapper<T, Integer> {
    public SecondaryCountsArrayCacheWrapper(CacheWrapperMeta meta) {
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
    }

    public static class SecondaryCountArrayCacheWrapper extends SecondaryCountsArrayCacheWrapper<Object> {
        public static final CacheType TYPE = CacheType.SECONDARY_COUNT;
        private static final int SUB_SIZE = 8;
        private final LimitHashArraySet arraySet;
        private int subStartOffset;

        public SecondaryCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20, SUB_SIZE, 3);
        }

        @Override
        public void updateStoreInfo(final CacheStore store, int offset) {
            super.updateStoreInfo(store, offset);
            this.subStartOffset = offset + 4;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(final String key) {
            return null;
        }

        @Override
        public int getCacheSize() {
            // first 4 byte: the count of sub keys
            // at most 20 sub keys
            // every sub key: 4 for hash, 4 for count
            return 4 + this.arraySet.getTotalBytesSize();
        }

        /**
         * @param value current value to compute.
         * @param keys  group keys' value.
         * @return positive count of a sub-key, or -1 if ignore.
         */
        @Override
        public Integer addData(final Object value, final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            // search the secondary key's hash
            try {
                int hash = HashType.getHash(keys[1]);
                byte[] target = this.cacheStore.getCache(keys[0]);

                int totalCount = ByteArrayUtil.getInt(this.offset, target);
                int bucketOffset = this.arraySet.find(hash, target, this.subStartOffset);
                if (bucketOffset >= 0) {
                    // find and update
                    return ByteArrayUtil.addInt(bucketOffset + 4, target, 1);
                } else {
                    if (totalCount < 20) {
                        // insert
                        int insertOffset = this.arraySet.add(hash, target, this.subStartOffset);
                        if (insertOffset >= 0) {
                            // add
                            ByteArrayUtil.addInt(this.offset, target, 1);
                            return ByteArrayUtil.putInt(insertOffset + 4, target, 1);
                        }
                    }
                    // ignore
                }

                // drop
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public final Integer getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }
            int hash = HashType.getHash(keys[1]);

            int targetOffset = this.arraySet.find(hash, target, this.subStartOffset);
            if (targetOffset > 0) {
                return ByteArrayUtil.getInt(targetOffset + 4, target);
            }
            return null;
        }
    }

    public static class SecondaryDistinctCountArrayCacheWrapper extends SecondaryCountsArrayCacheWrapper<String> {
        public static final CacheType TYPE = CacheType.SECONDARY_DISTINCT_COUNT;
        private final int subSize;

        private int subStartOffset;

        public SecondaryDistinctCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
            this.subSize = 88;
        }

        @Override
        public void updateStoreInfo(final CacheStore store, int offset) {
            super.updateStoreInfo(store, offset);
            this.subStartOffset = this.offset + 4;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(final String key) {
            return null;
        }

        @Override
        public int getCacheSize() {
            // 4: total recorded sub keys
            // max 20 sub keys
            // every sub key: 4 for hash, 4 for count, 20 * 4 for distinct value
            return 4 + 20 * 88;
        }

        @Override
        public Integer addData(final String value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }

            checkInitial(keys[0]);

            try {
                byte[] target = this.cacheStore.getCache(keys[0]);
                int subKeyHash = HashType.getMurMurHash(keys[1]);
                int subKeyTotal = ByteArrayUtil.getInt(this.offset, target);

                int subKeyOffset = ByteArrayUtil.binarySearchInt32(target, subStartOffset, subKeyTotal, subSize, subKeyHash);
                if (subKeyOffset > 0) {
                    // search distinct value
                    int valueHash = HashType.getMurMurHash(value);
                    // jump: 4 for sub-key hash, 4 for count
                    int valueStartOffset = subKeyOffset + 8;
                    int valueCount = ByteArrayUtil.getInt(subKeyOffset + 4, target);
                    int searchValueOffset = ByteArrayUtil.binarySearchInt32(target, valueStartOffset, valueCount > 20 ? 20 : valueCount, 4, valueHash);
                    if (searchValueOffset > 0) {
                        return valueCount;
                    } else {
                        if (valueCount >= 20) {
                            int after = addToHLL(subKeyHash, valueHash);
                            if (after + 20 > valueCount) {
                                return ByteArrayUtil.putInt(subKeyOffset + 4, target, after + 20);
                            } else {
                                return valueCount;
                            }
                        } else {
                            int insertOffset = searchValueOffset * -1;
                            ByteArrayUtil.insertInt32(target, valueStartOffset, insertOffset, valueCount, 4, valueHash);
                            return ByteArrayUtil.addInt(subKeyOffset + 4, target, 1);
                        }
                    }
                } else {
                    if (subKeyTotal < 20) {
                        // insert
                        int insertOffset = subKeyOffset * -1;
                        ByteArrayUtil.insertInt32(target, subStartOffset, insertOffset, subKeyTotal, subSize, subKeyHash);
                        // add sub-sub value
                        ByteArrayUtil.putInt(insertOffset + 8, target, HashType.getMurMurHash(value));
                        ByteArrayUtil.putInt(insertOffset + 4, target, 1);
                        // sub-key total increment
                        ByteArrayUtil.addInt(this.offset, target, 1);
                        return 1;
                    }
                    // ignore
                    return null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public final Integer getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }
            int subCount = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getMurMurHash(keys[1]);
            int searchOffset = ByteArrayUtil.binarySearchInt32(target, this.subStartOffset, subCount, subSize, hash);
            if (searchOffset > 0) {
                return ByteArrayUtil.getInt(searchOffset + 4, target);
            }
            return null;
        }

        private int addToHLL(int keyHash, int valueHash) {
            String hllKey = HLLUtil.getHLLKey(this.offset, keyHash);
            HLL hll = this.cacheStore.getHLL(hllKey);
            if (hll == null) {
                hll = this.cacheStore.allocateHLL(hllKey);
            }
            hll.addRaw(valueHash);
            return (int) hll.cardinality();
        }
    }

}

