package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.StringPool;
import com.threathunter.bordercollie.slot.util.HashType;
import net.agkn.hll.HLL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants.GLOBAL_KEY;

/**
 * Created by daisy on 17/3/22.
 */
public abstract class GlobalCountsArrayCacheWrapper<T, R> extends ByteArrayCacheWrapper<T, R> {

    public GlobalCountsArrayCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    public static class GlobalCountArrayCacheWrapper extends GlobalCountsArrayCacheWrapper<Object, Integer> {
        public static final CacheType TYPE = CacheType.GLOBAL_COUNT;

        public GlobalCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Integer addData(final Object value, final String... keys) {
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            return ByteArrayUtil.addInt(this.offset, target, 1);
        }

        @Override
        public Integer getData(final String... keys) {
            byte[] cache = this.cacheStore.getCache(GLOBAL_KEY);
            if (cache == null) {
                return null;
            }
            return ByteArrayUtil.getInt(this.offset, cache);
        }

        @Override
        public int getCacheSize() {
            return 4;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(final String key) {
            return null;
        }
    }

    // TODO add tests
    public static class GlobalGroupCountArrayCacheWrapper extends GlobalCountsArrayCacheWrapper<Object, Integer> {
        public static final CacheType TYPE = CacheType.GLOBAL_GROUP_COUNT;
        private final LimitHashArraySet arraySet;

        public GlobalGroupCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20, 8, 3);
        }

        @Override
        public Integer addData(Object value, String... keys) {
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (keys.length <= 0 || keys[0] == null) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            int hash = HashType.getHash(keys[0]);

            int currentOffset = this.arraySet.find(hash, target, this.offset);
            if (currentOffset >= 0) {
                // find and add count
                return ByteArrayUtil.addInt(currentOffset + 4, target, 1);
            }
            // need add
            // ignore if hash conflict (advance 3 times)
            int addOffset = this.arraySet.add(hash, target, this.offset);
            if (addOffset >= 0) {
                StringPool.getInstance().putString(hash, keys[0]);
                return ByteArrayUtil.addInt(addOffset + 4, target, 1);
            }

            return null;
        }

        @Override
        public Map getData(String... keys) {
            Map<String, Object> result = new HashMap<>();
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);
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

    public static class GlobalDistinctCountArrayCacheWrapper extends GlobalCountsArrayCacheWrapper<String, Integer> {
        public static final CacheType TYPE = CacheType.GLOBAL_DISTINCT_COUNT;
        private String hllKey;
        private final LimitHashArraySet arraySet;

        public GlobalDistinctCountArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20);
        }

        @Override
        public void updateStoreInfo(final CacheStore store, int offset) {
            super.updateStoreInfo(store, offset);
            this.hllKey = HLLUtil.getHLLKey(offset, GLOBAL_KEY);
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Integer addData(final String value, final String... keys) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            int hash = HashType.getHash(value);
            int currentCount = ByteArrayUtil.getInt(this.offset, target);

            if (this.arraySet.find(hash, target, this.offset + 4) >= 0) {
                return currentCount;
            }

            // need add
            // ignore if hash conflict (advance 3 times)
            if (this.arraySet.add(hash, target, this.offset + 4) < 0) {
                if (this.arraySet.getSize(target, this.offset + 4) >= 20) {
                    // add to hll
                    int after = addToHLL(HashType.getMurMurHash(value));
                    if (after + 20 > currentCount) {
                        return ByteArrayUtil.putInt(offset, target, after + 20);
                    }
                    return after + 20;
                } else {
                    // ignore hash collision
                    return currentCount;
                }
            }
            return ByteArrayUtil.addInt(this.offset, target, 1);
        }

        @Override
        public Integer getData(String... keys) {
            byte[] cache = this.cacheStore.getCache(GLOBAL_KEY);
            if (cache == null) {
                return null;
            }
            return ByteArrayUtil.getInt(this.offset, cache);
        }

        @Override
        public int getCacheSize() {
            return 4 + this.arraySet.getTotalBytesSize();
        }

        @Override
        public Object readAll(final String key) {
            return null;
        }

        private int addToHLL(int value) {
            HLL hll = this.cacheStore.getHLL(this.hllKey);
            if (hll == null) {
                hll = this.cacheStore.allocateHLL(this.hllKey);
            }
            hll.addRaw(value);
            return (int) hll.cardinality();
        }
    }
}
