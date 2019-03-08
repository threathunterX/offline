package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.StringPool;
import com.threathunter.bordercollie.slot.util.HashType;
import net.agkn.hll.HLL;

import java.util.*;

/**
 * Created by toyld on 3/21/17.
 */
public abstract class CountsArrayCacheWrapper<T, R> extends ByteArrayCacheWrapper<T, R> {
    public CountsArrayCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected final boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 1 || keys[0] == null) {
            return true;
        }
        return false;
    }

    public static class CountArrayCacheWrapper extends CountsArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.COUNT;

        public CountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public final Integer addData(final Object value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);

            return ByteArrayUtil.addInt(this.offset, target, 1);
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
            return this.getData(target, this.offset);
        }

        public Integer getData(final byte[] target, final int targetOffset) {
            return ByteArrayUtil.getInt(targetOffset, target);
        }

        @Override
        public int getCacheSize() {
            return 4;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            if (nullOrEmpty(keys)) {
                return lastMerged;
            }
            byte[] currentData = this.cacheStore.getCache(keys[0]);
            if (currentData == null) {
                return lastMerged;
            }
            return this.merge(lastMerged, currentData, this.offset);
        }

        public PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset) {
            PrimaryData merged;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return getData(getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[4]);
                ByteArrayUtil.putInt(0, merged.getRawData(), ByteArrayUtil.getInt(currentOffset, currentTarget));
            } else {
                merged = lastMerged;
                byte[] data = merged.getRawData();
                ByteArrayUtil.addInt(0, data, ByteArrayUtil.getInt(currentOffset, currentTarget));
            }

            return merged;
        }
    }

    // TODO add tests
    /**
     * Group count, value will be ignore, but keys need two, first is dimension key,
     * second is the group key.
     */
    public static class GroupCountArrayCacheWrapper extends CountsArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.GROUP_COUNT;
        private final LimitHashArraySet arraySet;

        public GroupCountArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20, 8, 3);
        }

        /**
         * Increment to update cache data.
         *
         * @param value do not care, will ignore
         * @param keys  must be 2 keys, first is for dimension key, second is for group tag.
         * @return
         */
        @Override
        public Integer addData(Object value, String... keys) {
            if (keys == null || keys.length < 2) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int hash = HashType.getHash(keys[1]);

            int currentOffset = this.arraySet.find(hash, target, this.offset);
            if (currentOffset >= 0) {
                // find and add count
                return ByteArrayUtil.addInt(currentOffset + 4, target, 1);
            }
            // need add
            // ignore if hash conflict (advance 3 times)
            int addOffset = this.arraySet.add(hash, target, this.offset);
            if (addOffset >= 0) {
                StringPool.getInstance().putString(hash, keys[1]);
                return ByteArrayUtil.addInt(addOffset + 4, target, 1);
            }

            return null;
        }

        @Override
        public Map getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            Map<String, Object> result = new HashMap<>();
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }
            List<LimitHashArraySet.SetEntry> entries = this.arraySet.getAll(target, this.offset);
            entries.forEach(entry -> {
                String origin = StringPool.getInstance().getString(entry.getHash());
                if (origin != null) {
                    result.put(origin, ByteArrayUtil.getInt(entry.getOffset() + 4, target));
                }
            });

            return result;
        }

        @Override
        public int getCacheSize() {
            return this.arraySet.getTotalBytesSize();
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

    /**
     * Since hashCode() in java is much more efficient than murmur hash, if store into LimitHashArraySet,
     * we will simply use hashCode(),
     * however, since hll require much more of hash, when add into hll, we will change to murmur hash,
     * since most time related user or did or others will be less than 20.
     * <p>
     * About hash collision: will retry next at most 3 times, or give up. This may cause result not so accurate.
     */
    public static class DistinctCountArrayCacheWrapper extends CountsArrayCacheWrapper<String, Integer> {
        public static final CacheType TYPE = CacheType.DISTINCT_COUNT;
        private final LimitHashArraySet arraySet;

        public DistinctCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20);
        }

        @Override
        public final Integer addData(final String value, final String... keys) {
            if (value == null || value.isEmpty() || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);

            int currentCount = ByteArrayUtil.getInt(this.offset, target);
            int hash = HashType.getHash(value);
            if (this.arraySet.find(hash, target, this.offset + 4) >= 0) {
                // found, return directly
                return currentCount;
            }
            // need add
            // add to hll if add failed
            if (this.arraySet.add(hash, target, this.offset + 4) < 0) {
                // add to hll
                int after = addToHLL(keys[0], HashType.getMurMurHash(value));
                int setSize = this.arraySet.getSize(target, this.offset + 4);
                if (after + setSize > currentCount) {
                    return ByteArrayUtil.putInt(offset, target, after + 20);
                }
                return after + 20;
            }
            return ByteArrayUtil.addInt(this.offset, target, 1);
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

            return this.getData(target, this.offset);
        }

        public Integer getData(final byte[] target, int targetOffset) {
            return ByteArrayUtil.getInt(targetOffset, target);
        }

        @Override
        public int getCacheSize() {
            return 4 + this.arraySet.getTotalBytesSize();
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        private int addToHLL(final String key, int value) {
            String hllKey = HLLUtil.getHLLKey(this.offset, key);
            HLL hll = this.cacheStore.getHLL(hllKey);
            if (hll == null) {
                hll = this.cacheStore.allocateHLL(hllKey);
            }
            hll.addRaw(value);
            return (int) hll.cardinality();
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            if (nullOrEmpty(keys)) {
                return lastMerged;
            }
            byte[] currentData = this.cacheStore.getCache(keys[0]);
            if (currentData == null) {
                return lastMerged;
            }
            return this.merge(lastMerged, currentData, this.offset, this.cacheStore.getHLL(HLLUtil.getHLLKey(this.offset, keys[0])));
        }

        public PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset, HLL currentHLL) {
            PrimaryData merged;
            HLL mergedHLL = null;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return getData(getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[this.getCacheSize() + HLLUtil.fixedSerializedSizeForHLL()]);
                if (currentHLL != null) {
                    byte[] hllBytes = currentHLL.toBytes();
                    mergedHLL = HLL.fromBytes(hllBytes);
                }
            } else {
                merged = lastMerged;
            }

            byte[] data = merged.getRawData();
            // first 4 is to record total size of the result.
            List<Integer> ignored = this.arraySet.merge(currentTarget, currentOffset + 4, merged.getRawData(), 4);
            if (ignored.size() > 0 || currentHLL != null) {
                // merge or create HLL
                if (merged.getRawData()[this.getCacheSize()] == 0) {
                    // create a new HLL
                    mergedHLL = HLLUtil.createHLL();
                } else {
                    mergedHLL = HLLUtil.fromHLL(merged.getRawData(), this.getCacheSize());
                }
                if (currentHLL != null) {
                    mergedHLL.union(currentHLL);
                }
                for (int hash : ignored) {
                    mergedHLL.addRaw(hash);
                }
            }

            int size = this.arraySet.getSize(merged.getRawData(), 4);
            if (mergedHLL != null) {
                size += mergedHLL.cardinality();
                byte[] hllBytes = mergedHLL.toBytes();
                System.arraycopy(hllBytes, 0, merged.getRawData(), this.getCacheSize(), hllBytes.length);
            }
            ByteArrayUtil.putInt(0, data, size);

            return merged;
        }
    }
}

